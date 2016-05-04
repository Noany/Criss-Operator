/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.beans.Introspector
import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.serializer.Serializer
import org.apache.spark.sql._
import org.apache.spark.sql.auto.cache.QGDriver
import org.apache.spark.sql.auto.cache.QGUtils.NodeDesc
import org.apache.spark.storage.StorageLevel
import tachyon.thrift.BenefitInfo


import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.collection.mutable.{HashMap, ArrayBuffer}
import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.{TachyonRDD, RDD}
import org.apache.spark.sql.SQLConf.SQLConfEntry
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.errors.DialectException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.{InternalRow, ParserDialect, _}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.ui.{SQLListener, SQLTab}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * The entry point for working with structured data (rows and columns) in Spark.  Allows the
 * creation of [[DataFrame]] objects as well as the execution of SQL queries.
 *
 * @groupname basic Basic Operations
 * @groupname ddl_ops Persistent Catalog DDL
 * @groupname cachemgmt Cached Table Management
 * @groupname genericdata Generic Data Sources
 * @groupname specificdata Specific Data Sources
 * @groupname config Configuration
 * @groupname dataframes Custom DataFrame Creation
 * @groupname Ungrouped Support functions for language integrated queries.
 *
 * @since 1.0.0
 */
class SQLContext(@transient val sparkContext: SparkContext)
  extends org.apache.spark.Logging
  with Serializable {

  self =>

  //zengdan
  protected[sql]  lazy val (actorSystem, qgDriver) = QGDriver.createActor(sparkContext)

  //zengdan
  lazy val idToBenefit = scala.collection.mutable.Map[Int, BenefitInfo]()


  //zengdan lazy
  def cacheData(output: Seq[Attribute], id: Int): Boolean = {
    val cache = QGDriver.saveSchema(output, id, this, this.qgDriver)
    if (cache) idToBenefit.put(id, QGDriver.getBenefit(id, this, this.qgDriver))
    cache
  }

  //zengdan
  def loadData(output: Seq[Attribute], nodeRef: Option[QNodeRef], backupRdd: RDD[InternalRow]): RDD[InternalRow] = {
    var loaded: RDD[InternalRow] = backupRdd
    if(nodeRef.isDefined && nodeRef.get.reuse) {
      val data: Option[RDD[InternalRow]] =
        this.sparkContext.loadCachedFile[InternalRow](nodeRef.get.id)
      if (data.isDefined) {
        //zd reuse data
        idToBenefit.put(nodeRef.get.id, QGDriver.getBenefit(nodeRef.get.id, this, this.qgDriver))
        val schema = QGDriver.getSchema(nodeRef.get.id, this, this.qgDriver)
        loaded = SQLContext.projectLoadedData(backupRdd, data.get, schema.map(_.name), output)
      }
    }
    loaded
  }

  //zengdan
  lazy val serializer = {
    val conf = sparkContext.getConf
    val className = conf.get("spark.sql.plan.serializer", "org.apache.spark.serializer.JavaSerializer")
    val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
    try {
      cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[Serializer]
    } catch {
      case _: NoSuchMethodException =>
        cls.getConstructor().newInstance().asInstanceOf[Serializer]
    }
  }

  //zengdan
  var planId = 0
  //for micro metrics
  var needCollect = 0
  var needCache = 0
  var needReuse = 0
  var total = 0
  //

  val cachedNode = new ArrayBuffer[Int]()
  val reusedNode = new ArrayBuffer[Int]()

  //zengdan
  def getOptimizedPlan(plan: SparkPlan) = {
    if(this.sparkContext.getConf.get("spark.sql.auto.cache", "false").toBoolean){
      planId = makeIdForPlan(plan, planId)
      //recoveryContext(plan, QGDriver.rewrittenPlan(plan, this, qgDriver))
      val planUpdate = QGDriver.rewrittenPlan(plan, this, qgDriver)
      needCollect = 0
      needCache = 0
      needReuse = 0
      total = 0
      val newPlan = updatePlan(plan, planUpdate.refs, planUpdate.addNodes)
      //mm
      logError(this.sparkContext.applicationId + " Total Operator: " + total + " Collect: " + needCollect + " Cache: " + needCache + " Reuse: " + needReuse)
      //mm
      newPlan
      //setNodeRef(plan, QGDriver.rewrittenPlan(plan, this, qgDriver))
    }else
      plan
  }

  //zengdan
  def makeIdForPlan(plan: SparkPlan, startId: Int): Int = {
    if(plan != null){
      var id = startId
      for(child <- plan.children){
        id = makeIdForPlan(child, id)
      }
      plan.id = id
      id+1
    }else {
      startId
    }
  }

  //zengdan
  def updatePlan(plan: SparkPlan, refs: HashMap[Int, QNodeRef],
                 added: HashMap[Int, ArrayBuffer[NodeDesc]]): SparkPlan = {
    if(plan == null) return null

    val oldChildren = plan.children
    val childrenBuffer = new ArrayBuffer[SparkPlan]()
    for (child <- oldChildren) {
      childrenBuffer.append(updatePlan(child, refs, added))
    }

    if(refs.get(plan.id).isDefined){
      val nr = refs.get(plan.id).get
      //plan.nodeRef = Some(QNodeRef(nr.id, nr.cache, nr.collect, nr.reuse, nr.existTime))
      plan.updateNodeRef(Some(QNodeRef(nr.id, nr.cache, nr.collect, nr.reuse, nr.existTime)))
      //mm
      if (nr.cache) {
        cachedNode += nr.id
        needCache += 1
      }
      if (nr.collect) needCollect += 1
      if (nr.reuse) {
        needReuse += 1
        reusedNode += nr.id
      }
      total += 1
      //mm
    }

    //children need to add
    val children = added.get(plan.id).getOrElse(Nil)
    if(!children.isEmpty){
      var i = 0
      if(children.length == 1){
        //val child = plan.getClass.getConstructor(plan.args: _*)
        val child = plan.getClass.getConstructors.find(_.getParameterTypes.size != 0).head
          .newInstance((children(0).args ++ childrenBuffer).toArray:_*).asInstanceOf[plan.type]
        //child.nodeRef = Some(children(0).nodeRef)
        child.updateNodeRef(Some(children(0).nodeRef))
        val newPlan = plan.withNewChildren(Seq(child))
        newPlan.updateNodeRef(plan.nodeRef)
        //newPlan.nodeRef = plan.nodeRef
        return newPlan
      }else{
        //combine child operator
      }
    }

    val newPlan = plan.withNewChildren(childrenBuffer)
    //newPlan.nodeRef = plan.nodeRef
    newPlan.updateNodeRef(plan.nodeRef)
    newPlan
  }

  /*
  //zengdan
  def setNodeRef(plan: SparkPlan, refs: HashMap[Int, QNodeRef]) {
    if (plan != null) {
      if (refs.get(plan.id).isDefined) {
        plan.nodeRef = Some(refs.get(plan.id).get)
      }
      for (child <- plan.children) {
        setNodeRef(child, refs)
      }
    }
  }
  */

  /*
  //zengdan
  private def getTableRDD(plan: LogicalPlan): Map[Int,LogicalRDD] = {
    var rdds = Map[Int,LogicalRDD]()
    if(plan.isInstanceOf[LogicalRDD]){
      val lr = plan.asInstanceOf[LogicalRDD]
      rdds += (lr.rdd.id->lr)
    }else{
      for(child <- plan.children){
        rdds ++= getTableRDD(child)
      }
    }
    rdds
  }
  */

  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  /**
   * @return Spark SQL configuration
   */
  protected[sql] def conf = currentSession().conf

  // `listener` should be only used in the driver
  @transient private[sql] val listener = new SQLListener(this)
  sparkContext.addSparkListener(listener)
  sparkContext.ui.foreach(new SQLTab(this, _))

  // Execution IDs go through SparkContext's local properties, which are not safe to use with
  // fork join pools by default. In particular, even after a child thread is spawned, if the
  // parent sets a property the value may be reflected in the child. This leads to undefined
  // consequences such as SPARK-10548, so we should just clone the properties instead to be safe.
  sparkContext.conf.set("spark.localProperties.clone", "true")

  /**
   * Set Spark SQL configuration properties.
   *
   * @group config
   * @since 1.0.0
   */
  def setConf(props: Properties): Unit = conf.setConf(props)

  /** Set the given Spark SQL configuration property. */
  private[sql] def setConf[T](entry: SQLConfEntry[T], value: T): Unit = conf.setConf(entry, value)

  /**
   * Set the given Spark SQL configuration property.
   *
   * @group config
   * @since 1.0.0
   */
  def setConf(key: String, value: String): Unit = conf.setConfString(key, value)

  /**
   * Return the value of Spark SQL configuration property for the given key.
   *
   * @group config
   * @since 1.0.0
   */
  def getConf(key: String): String = conf.getConfString(key)

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue` in [[SQLConfEntry]].
   */
  private[sql] def getConf[T](entry: SQLConfEntry[T]): T = conf.getConf(entry)

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`. This is useful when `defaultValue` in SQLConfEntry is not the
   * desired one.
   */
  private[sql] def getConf[T](entry: SQLConfEntry[T], defaultValue: T): T = {
    conf.getConf(entry, defaultValue)
  }

  /**
   * Return the value of Spark SQL configuration property for the given key. If the key is not set
   * yet, return `defaultValue`.
   *
   * @group config
   * @since 1.0.0
   */
  def getConf(key: String, defaultValue: String): String = conf.getConfString(key, defaultValue)

  /**
   * Return all the configuration properties that have been set (i.e. not the default).
   * This creates a new copy of the config properties in the form of a Map.
   *
   * @group config
   * @since 1.0.0
   */
  def getAllConfs: immutable.Map[String, String] = conf.getAllConfs

  // TODO how to handle the temp table per user session?
  @transient
  protected[sql] lazy val catalog: Catalog = new SimpleCatalog(conf)

  // TODO how to handle the temp function per user session?
  @transient
  protected[sql] lazy val functionRegistry: FunctionRegistry = FunctionRegistry.builtin

  @transient
  protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        ExtractPythonUDFs ::
        PreInsertCastAndRename ::
        Nil

      override val extendedCheckRules = Seq(
        datasources.PreWriteCheck(catalog)
      )
    }

  @transient
  protected[sql] lazy val optimizer: Optimizer = DefaultOptimizer

  @transient
  protected[sql] val ddlParser = new DDLParser(sqlParser.parse(_))

  @transient
  protected[sql] val sqlParser = new SparkSQLParser(getSQLDialect().parse(_))

  protected[sql] def getSQLDialect(): ParserDialect = {
    try {
      val clazz = Utils.classForName(dialectClassName)
      clazz.newInstance().asInstanceOf[ParserDialect]
    } catch {
      case NonFatal(e) =>
        // Since we didn't find the available SQL Dialect, it will fail even for SET command:
        // SET spark.sql.dialect=sql; Let's reset as default dialect automatically.
        val dialect = conf.dialect
        // reset the sql dialect
        conf.unsetConf(SQLConf.DIALECT)
        // throw out the exception, and the default sql dialect will take effect for next query.
        throw new DialectException(
          s"""Instantiating dialect '$dialect' failed.
             |Reverting to default dialect '${conf.dialect}'""".stripMargin, e)
    }
  }

  protected[sql] def parseSql(sql: String): LogicalPlan = ddlParser.parse(sql, false)

  protected[sql] def executeSql(sql: String): this.QueryExecution = executePlan(parseSql(sql))

  protected[sql] def executePlan(plan: LogicalPlan) = new this.QueryExecution(plan)

  @transient
  protected[sql] val tlSession = new ThreadLocal[SQLSession]() {
    override def initialValue: SQLSession = defaultSession
  }

  @transient
  protected[sql] val defaultSession = createSession()

  protected[sql] def dialectClassName = if (conf.dialect == "sql") {
    classOf[DefaultParserDialect].getCanonicalName
  } else {
    conf.dialect
  }

  {
    // We extract spark sql settings from SparkContext's conf and put them to
    // Spark SQL's conf.
    // First, we populate the SQLConf (conf). So, we can make sure that other values using
    // those settings in their construction can get the correct settings.
    // For example, metadataHive in HiveContext may need both spark.sql.hive.metastore.version
    // and spark.sql.hive.metastore.jars to get correctly constructed.
    val properties = new Properties
    sparkContext.getConf.getAll.foreach {
      case (key, value) if key.startsWith("spark.sql") => properties.setProperty(key, value)
      case _ =>
    }
    // We directly put those settings to conf to avoid of calling setConf, which may have
    // side-effects. For example, in HiveContext, setConf may cause executionHive and metadataHive
    // get constructed. If we call setConf directly, the constructed metadataHive may have
    // wrong settings, or the construction may fail.
    conf.setConf(properties)
    // After we have populated SQLConf, we call setConf to populate other confs in the subclass
    // (e.g. hiveconf in HiveContext).
    properties.foreach {
      case (key, value) => setConf(key, value)
    }
  }

  @transient
  protected[sql] val cacheManager = new CacheManager(this)

  /**
   * :: Experimental ::
   * A collection of methods that are considered experimental, but can be used to hook into
   * the query planner for advanced functionality.
   *
   * @group basic
   * @since 1.3.0
   */
  @Experimental
  @transient
  val experimental: ExperimentalMethods = new ExperimentalMethods(this)

  /**
   * :: Experimental ::
   * Returns a [[DataFrame]] with no rows or columns.
   *
   * @group basic
   * @since 1.3.0
   */
  @Experimental
  @transient
  lazy val emptyDataFrame: DataFrame = createDataFrame(sparkContext.emptyRDD[Row], StructType(Nil))

  /**
   * A collection of methods for registering user-defined functions (UDF).
   *
   * The following example registers a Scala closure as UDF:
   * {{{
   *   sqlContext.udf.register("myUDF", (arg1: Int, arg2: String) => arg2 + arg1)
   * }}}
   *
   * The following example registers a UDF in Java:
   * {{{
   *   sqlContext.udf().register("myUDF",
   *       new UDF2<Integer, String, String>() {
   *           @Override
   *           public String call(Integer arg1, String arg2) {
   *               return arg2 + arg1;
   *           }
   *      }, DataTypes.StringType);
   * }}}
   *
   * Or, to use Java 8 lambda syntax:
   * {{{
   *   sqlContext.udf().register("myUDF",
   *       (Integer arg1, String arg2) -> arg2 + arg1,
   *       DataTypes.StringType);
   * }}}
   *
   * @group basic
   * @since 1.3.0
   * TODO move to SQLSession?
   */
  @transient
  val udf: UDFRegistration = new UDFRegistration(this)

  /**
   * Returns true if the table is currently cached in-memory.
   * @group cachemgmt
   * @since 1.3.0
   */
  def isCached(tableName: String): Boolean = cacheManager.isCached(tableName)

  /**
   * Caches the specified table in-memory.
   * @group cachemgmt
   * @since 1.3.0
   */
  def cacheTable(tableName: String): Unit = cacheManager.cacheTable(tableName)

  /**
   * Removes the specified table from the in-memory cache.
   * @group cachemgmt
   * @since 1.3.0
   */
  def uncacheTable(tableName: String): Unit = cacheManager.uncacheTable(tableName)

  /**
   * Removes all cached tables from the in-memory cache.
   * @since 1.3.0
   */
  def clearCache(): Unit = cacheManager.clearCache()

  // scalastyle:off
  // Disable style checker so "implicits" object can start with lowercase i
  /**
   * :: Experimental ::
   * (Scala-specific) Implicit methods available in Scala for converting
   * common Scala objects into [[DataFrame]]s.
   *
   * {{{
   *   val sqlContext = new SQLContext(sc)
   *   import sqlContext.implicits._
   * }}}
   *
   * @group basic
   * @since 1.3.0
   */
  @Experimental
  object implicits extends SQLImplicits with Serializable {
    protected override def _sqlContext: SQLContext = self

    /**
     * Converts $"col name" into an [[Column]].
     * @since 1.3.0
     */
    // This must live here to preserve binary compatibility with Spark < 1.5.
    implicit class StringToColumn(val sc: StringContext) {
      def $(args: Any*): ColumnName = {
        new ColumnName(sc.s(args: _*))
      }
    }
  }
  // scalastyle:on

  /**
   * :: Experimental ::
   * Creates a DataFrame from an RDD of Product (e.g. case classes, tuples).
   *
   * @group dataframes
   * @since 1.3.0
   */
  @Experimental
  def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame = {
    SparkPlan.currentContext.set(self)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val rowRDD = RDDConversions.productToRowRdd(rdd, schema.map(_.dataType))
    DataFrame(self, LogicalRDD(attributeSeq, rowRDD)(self))
  }

  /**
   * :: Experimental ::
   * Creates a DataFrame from a local Seq of Product.
   *
   * @group dataframes
   * @since 1.3.0
   */
  @Experimental
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame = {
    SparkPlan.currentContext.set(self)
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    DataFrame(self, LocalRelation.fromProduct(attributeSeq, data))
  }

  /**
   * Convert a [[BaseRelation]] created for external data sources into a [[DataFrame]].
   *
   * @group dataframes
   * @since 1.3.0
   */
  def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame = {
    DataFrame(this, LogicalRelation(baseRelation))
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from an [[RDD]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   * Example:
   * {{{
   *  import org.apache.spark.sql._
   *  import org.apache.spark.sql.types._
   *  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   *
   *  val schema =
   *    StructType(
   *      StructField("name", StringType, false) ::
   *      StructField("age", IntegerType, true) :: Nil)
   *
   *  val people =
   *    sc.textFile("examples/src/main/resources/people.txt").map(
   *      _.split(",")).map(p => Row(p(0), p(1).trim.toInt))
   *  val dataFrame = sqlContext.createDataFrame(people, schema)
   *  dataFrame.printSchema
   *  // root
   *  // |-- name: string (nullable = false)
   *  // |-- age: integer (nullable = true)
   *
   *  dataFrame.registerTempTable("people")
   *  sqlContext.sql("select name from people").collect.foreach(println)
   * }}}
   *
   * @group dataframes
   * @since 1.3.0
   */
  @DeveloperApi
  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema, needsConversion = true)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def createDataFrame(rowRDD: RDD[Row], schema: StructType, needsConversion: Boolean) = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val catalystRows = if (needsConversion) {
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowRDD.map(converter(_).asInstanceOf[InternalRow])
    } else {
      rowRDD.map{r: Row => InternalRow.fromSeq(r.toSeq)}
    }
    val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
    DataFrame(this, logicalPlan)
  }

  /**
   * Creates a DataFrame from an RDD[Row]. User can specify whether the input rows should be
   * converted to Catalyst rows.
   */
  private[sql]
  def internalCreateDataFrame(catalystRows: RDD[InternalRow], schema: StructType) = {
    // TODO: use MutableProjection when rowRDD is another DataFrame and the applied
    // schema differs from the existing schema on any field data type.
    val logicalPlan = LogicalRDD(schema.toAttributes, catalystRows)(self)
    DataFrame(this, logicalPlan)
  }

  /**
   * :: DeveloperApi ::
   * Creates a [[DataFrame]] from an [[JavaRDD]] containing [[Row]]s using the given schema.
   * It is important to make sure that the structure of every [[Row]] of the provided RDD matches
   * the provided schema. Otherwise, there will be runtime exception.
   *
   * @group dataframes
   * @since 1.3.0
   */
  @DeveloperApi
  def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD.rdd, schema)
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @group dataframes
   * @since 1.3.0
   */
  def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
    val attributeSeq = getSchema(beanClass)
    val className = beanClass.getName
    val rowRdd = rdd.mapPartitions { iter =>
      // BeanInfo is not serializable so we must rediscover it remotely for each partition.
      val localBeanInfo = Introspector.getBeanInfo(Utils.classForName(className))
      val extractors =
        localBeanInfo.getPropertyDescriptors.filterNot(_.getName == "class").map(_.getReadMethod)
      val methodsToConverts = extractors.zip(attributeSeq).map { case (e, attr) =>
        (e, CatalystTypeConverters.createToCatalystConverter(attr.dataType))
      }
      iter.map { row =>
        new GenericInternalRow(
          methodsToConverts.map { case (e, convert) => convert(e.invoke(row)) }.toArray[Any]
        ): InternalRow
      }
    }
    DataFrame(this, LogicalRDD(attributeSeq, rowRdd)(this))
  }

  /**
   * Applies a schema to an RDD of Java Beans.
   *
   * WARNING: Since there is no guaranteed ordering for fields in a Java Bean,
   *          SELECT * queries will return the columns in an undefined order.
   * @group dataframes
   * @since 1.3.0
   */
  def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd.rdd, beanClass)
  }

  /**
   * :: Experimental ::
   * Returns a [[DataFrameReader]] that can be used to read data in as a [[DataFrame]].
   * {{{
   *   sqlContext.read.parquet("/path/to/file.parquet")
   *   sqlContext.read.schema(schema).json("/path/to/file.json")
   * }}}
   *
   * @group genericdata
   * @since 1.4.0
   */
  @Experimental
  def read: DataFrameReader = new DataFrameReader(this)

  /**
   * :: Experimental ::
   * Creates an external table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(tableName: String, path: String): DataFrame = {
    val dataSourceName = conf.defaultDataSourceName
    createExternalTable(tableName, path, dataSourceName)
  }

  /**
   * :: Experimental ::
   * Creates an external table from the given path based on a data source
   * and returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      path: String,
      source: String): DataFrame = {
    createExternalTable(tableName, source, Map("path" -> path))
  }

  /**
   * :: Experimental ::
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      options: java.util.Map[String, String]): DataFrame = {
    createExternalTable(tableName, source, options.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates an external table from the given path based on a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    val tableIdent = SqlParser.parseTableIdentifier(tableName)
    val cmd =
      CreateTableUsing(
        tableIdent,
        userSpecifiedSchema = None,
        source,
        temporary = false,
        options,
        allowExisting = false,
        managedIfNoPath = false)
    executePlan(cmd).toRdd
    table(tableIdent)
  }

  /**
   * :: Experimental ::
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: java.util.Map[String, String]): DataFrame = {
    createExternalTable(tableName, source, schema, options.toMap)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Create an external table from the given path based on a data source, a schema and
   * a set of options. Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  @Experimental
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    val tableIdent = SqlParser.parseTableIdentifier(tableName)
    val cmd =
      CreateTableUsing(
        tableIdent,
        userSpecifiedSchema = Some(schema),
        source,
        temporary = false,
        options,
        allowExisting = false,
        managedIfNoPath = false)
    executePlan(cmd).toRdd
    table(tableIdent)
  }

  /**
   * Registers the given [[DataFrame]] as a temporary table in the catalog. Temporary tables exist
   * only during the lifetime of this instance of SQLContext.
   */
  private[sql] def registerDataFrameAsTable(df: DataFrame, tableName: String): Unit = {
    catalog.registerTable(Seq(tableName), df.logicalPlan)
  }

  /**
   * Drops the temporary table with the given table name in the catalog. If the table has been
   * cached/persisted before, it's also unpersisted.
   *
   * @param tableName the name of the table to be unregistered.
   *
   * @group basic
   * @since 1.3.0
   */
  def dropTempTable(tableName: String): Unit = {
    cacheManager.tryUncacheQuery(table(tableName))
    catalog.unregisterTable(Seq(tableName))
  }

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in an range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 1.4.1
   * @group dataframe
   */
  @Experimental
  def range(end: Long): DataFrame = range(0, end)

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in an range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 1.4.0
   * @group dataframe
   */
  @Experimental
  def range(start: Long, end: Long): DataFrame = {
    createDataFrame(
      sparkContext.range(start, end).map(Row(_)),
      StructType(StructField("id", LongType, nullable = false) :: Nil))
  }

  /**
   * :: Experimental ::
   * Creates a [[DataFrame]] with a single [[LongType]] column named `id`, containing elements
   * in an range from `start` to `end` (exclusive) with an step value, with partition number
   * specified.
   *
   * @since 1.4.0
   * @group dataframe
   */
  @Experimental
  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame = {
    createDataFrame(
      sparkContext.range(start, end, step, numPartitions).map(Row(_)),
      StructType(StructField("id", LongType, nullable = false) :: Nil))
  }

  /**
   * Executes a SQL query using Spark, returning the result as a [[DataFrame]]. The dialect that is
   * used for SQL parsing can be configured with 'spark.sql.dialect'.
   *
   * @group basic
   * @since 1.3.0
   */
  def sql(sqlText: String): DataFrame = {
    DataFrame(this, parseSql(sqlText))
  }

  /**
   * Returns the specified table as a [[DataFrame]].
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def table(tableName: String): DataFrame = {
    table(SqlParser.parseTableIdentifier(tableName))
  }

  private def table(tableIdent: TableIdentifier): DataFrame = {
    DataFrame(this, catalog.lookupRelation(tableIdent.toSeq))
  }

  /**
   * Returns a [[DataFrame]] containing names of existing tables in the current database.
   * The returned DataFrame has two columns, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tables(): DataFrame = {
    DataFrame(this, ShowTablesCommand(None))
  }

  /**
   * Returns a [[DataFrame]] containing names of existing tables in the given database.
   * The returned DataFrame has two columns, tableName and isTemporary (a Boolean
   * indicating if a table is a temporary one or not).
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tables(databaseName: String): DataFrame = {
    DataFrame(this, ShowTablesCommand(Some(databaseName)))
  }

  /**
   * Returns the names of tables in the current database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(): Array[String] = {
    catalog.getTables(None).map {
      case (tableName, _) => tableName
    }.toArray
  }

  /**
   * Returns the names of tables in the given database as an array.
   *
   * @group ddl_ops
   * @since 1.3.0
   */
  def tableNames(databaseName: String): Array[String] = {
    catalog.getTables(Some(databaseName)).map {
      case (tableName, _) => tableName
    }.toArray
  }

  protected[sql] class SparkPlanner extends SparkStrategies {
    val sparkContext: SparkContext = self.sparkContext

    val sqlContext: SQLContext = self

    def codegenEnabled: Boolean = self.conf.codegenEnabled

    def unsafeEnabled: Boolean = self.conf.unsafeEnabled

    def numPartitions: Int = self.conf.numShufflePartitions

    def strategies: Seq[Strategy] =
      experimental.extraStrategies ++ (
      DataSourceStrategy ::
      DDLStrategy ::
      TakeOrderedAndProject ::
      HashAggregation ::
      Aggregation ::
      LeftSemiJoin ::
      EquiJoinSelection ::
      InMemoryScans ::
      BasicOperators ::
      CartesianProduct ::
      BroadcastNestedLoopJoin :: Nil)

    /**
     * Used to build table scan operators where complex projection and filtering are done using
     * separate physical operators.  This function returns the given scan operator with Project and
     * Filter nodes added only when needed.  For example, a Project operator is only used when the
     * final desired output requires complex expressions to be evaluated or when columns can be
     * further eliminated out after filtering has been done.
     *
     * The `prunePushedDownFilters` parameter is used to remove those filters that can be optimized
     * away by the filter pushdown optimization.
     *
     * The required attributes for both filtering and expression evaluation are passed to the
     * provided `scanBuilder` function so that it can avoid unnecessary column materialization.
     */
    def pruneFilterProject(
        projectList: Seq[NamedExpression],
        filterPredicates: Seq[Expression],
        prunePushedDownFilters: Seq[Expression] => Seq[Expression],
        scanBuilder: Seq[Attribute] => SparkPlan): SparkPlan = {

      val projectSet = AttributeSet(projectList.flatMap(_.references))
      val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
      val filterCondition =
        prunePushedDownFilters(filterPredicates).reduceLeftOption(catalyst.expressions.And)

      // Right now we still use a projection even if the only evaluation is applying an alias
      // to a column.  Since this is a no-op, it could be avoided. However, using this
      // optimization with the current implementation would change the output schema.
      // TODO: Decouple final output schema from expression evaluation so this copy can be
      // avoided safely.

      if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
          filterSet.subsetOf(projectSet)) {
        // When it is possible to just use column pruning to get the right projection and
        // when the columns of this projection are enough to evaluate all filter conditions,
        // just do a scan followed by a filter, with no extra project.
        val scan = scanBuilder(projectList.asInstanceOf[Seq[Attribute]])
        filterCondition.map(Filter(_, scan)).getOrElse(scan)
      } else {
        val scan = scanBuilder((projectSet ++ filterSet).toSeq)
        Project(projectList, filterCondition.map(Filter(_, scan)).getOrElse(scan)) //zengdan for test
      }
    }
  }

  @transient
  protected[sql] val planner = new SparkPlanner

  @transient
  protected[sql] lazy val emptyResult = sparkContext.parallelize(Seq.empty[InternalRow], 1)

  /**
   * Prepares a planned SparkPlan for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches = Seq(
      Batch("Add exchange", Once, EnsureRequirements(self)),
      Batch("Add row converters", Once, EnsureRowFormats)
    )
  }

  protected[sql] def openSession(): SQLSession = {
    detachSession()
    val session = createSession()
    tlSession.set(session)

    session
  }

  protected[sql] def currentSession(): SQLSession = {
    tlSession.get()
  }

  protected[sql] def createSession(): SQLSession = {
    new this.SQLSession()
  }

  protected[sql] def detachSession(): Unit = {
    tlSession.remove()
  }

  protected[sql] def setSession(session: SQLSession): Unit = {
    detachSession()
    tlSession.set(session)
  }

  protected[sql] class SQLSession {
    // Note that this is a lazy val so we can override the default value in subclasses.
    protected[sql] lazy val conf: SQLConf = new SQLConf
  }

  /**
   * :: DeveloperApi ::
   * The primary workflow for executing relational queries using Spark.  Designed to allow easy
   * access to the intermediate phases of query execution for developers.
   */
  @DeveloperApi
  protected[sql] class QueryExecution(val logical: LogicalPlan) {
    def assertAnalyzed(): Unit = analyzer.checkAnalysis(analyzed)

    lazy val analyzed: LogicalPlan = analyzer.execute(logical)
    lazy val withCachedData: LogicalPlan = {
      assertAnalyzed()
      cacheManager.useCachedData(analyzed)
    }
    lazy val optimizedPlan: LogicalPlan = optimizer.execute(withCachedData)

    // TODO: Don't just pick the first one...
    lazy val sparkPlan: SparkPlan = {
      SparkPlan.currentContext.set(self)
      planner.plan(optimizedPlan).next()
    }
    // executedPlan should not be used to initialize any SparkPlan. It should be
    // only used for execution.
    lazy val executedPlan: SparkPlan = {
      val plan = prepareForExecution.execute(sparkPlan)
      //zengdan
      getOptimizedPlan(plan)
    }

    /** Internal version of the RDD. Avoids copies and has no schema */
    lazy val toRdd: RDD[InternalRow] = executedPlan.execute()

    protected def stringOrError[A](f: => A): String =
      try f.toString catch { case e: Throwable => e.toString }

    def simpleString: String =
      s"""== Physical Plan ==
         |${stringOrError(executedPlan)}
      """.stripMargin.trim

    override def toString: String = {
      def output =
        analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}").mkString(", ")

      s"""== Parsed Logical Plan ==
         |${stringOrError(logical)}
         |== Analyzed Logical Plan ==
         |${stringOrError(output)}
         |${stringOrError(analyzed)}
         |== Optimized Logical Plan ==
         |${stringOrError(optimizedPlan)}
         |== Physical Plan ==
         |${stringOrError(executedPlan)}
         |Code Generation: ${stringOrError(executedPlan.codegenEnabled)}
      """.stripMargin.trim
    }
  }

  /**
   * Parses the data type in our internal string representation. The data type string should
   * have the same format as the one generated by `toString` in scala.
   * It is only used by PySpark.
   */
  protected[sql] def parseDataType(dataTypeString: String): DataType = {
    DataType.fromJson(dataTypeString)
  }

  /**
   * Apply a schema defined by the schemaString to an RDD. It is only used by PySpark.
   */
  protected[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schemaString: String): DataFrame = {
    val schema = parseDataType(schemaString).asInstanceOf[StructType]
    applySchemaToPythonRDD(rdd, schema)
  }

  /**
   * Apply a schema defined by the schema to an RDD. It is only used by PySpark.
   */
  protected[sql] def applySchemaToPythonRDD(
      rdd: RDD[Array[Any]],
      schema: StructType): DataFrame = {

    val rowRdd = rdd.map(r => EvaluatePython.fromJava(r, schema).asInstanceOf[InternalRow])
    DataFrame(this, LogicalRDD(schema.toAttributes, rowRdd)(self))
  }

  /**
   * Returns a Catalyst Schema for the given java bean class.
   */
  protected def getSchema(beanClass: Class[_]): Seq[AttributeReference] = {
    val (dataType, _) = JavaTypeInference.inferDataType(beanClass)
    dataType.asInstanceOf[StructType].fields.map { f =>
      AttributeReference(f.name, f.dataType, f.nullable)()
    }
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // Deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`.
   */
  @deprecated("use createDataFrame", "1.3.0")
  def applySchema(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`.
   */
  @deprecated("use createDataFrame", "1.3.0")
  def applySchema(rowRDD: JavaRDD[Row], schema: StructType): DataFrame = {
    createDataFrame(rowRDD, schema)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`.
   */
  @deprecated("use createDataFrame", "1.3.0")
  def applySchema(rdd: RDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd, beanClass)
  }

  /**
   * @deprecated As of 1.3.0, replaced by `createDataFrame()`.
   */
  @deprecated("use createDataFrame", "1.3.0")
  def applySchema(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame = {
    createDataFrame(rdd, beanClass)
  }

  /**
   * Loads a Parquet file, returning the result as a [[DataFrame]]. This function returns an empty
   * [[DataFrame]] if no paths are passed in.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().parquet()`.
   */
  @deprecated("Use read.parquet()", "1.4.0")
  @scala.annotation.varargs
  def parquetFile(paths: String*): DataFrame = {
    if (paths.isEmpty) {
      emptyDataFrame
    } else {
      read.parquet(paths : _*)
    }
  }

  /**
   * Loads a JSON file (one object per line), returning the result as a [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonFile(path: String): DataFrame = {
    read.json(path)
  }

  /**
   * Loads a JSON file (one object per line) and applies the given schema,
   * returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonFile(path: String, schema: StructType): DataFrame = {
    read.schema(schema).json(path)
  }

  /**
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonFile(path: String, samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(path)
  }

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonRDD(json: RDD[String]): DataFrame = read.json(json)

  /**
   * Loads an RDD[String] storing JSON objects (one object per record), returning the result as a
   * [[DataFrame]].
   * It goes through the entire dataset once to determine the schema.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonRDD(json: JavaRDD[String]): DataFrame = read.json(json)

  /**
   * Loads an RDD[String] storing JSON objects (one object per record) and applies the given schema,
   * returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonRDD(json: RDD[String], schema: StructType): DataFrame = {
    read.schema(schema).json(json)
  }

  /**
   * Loads an JavaRDD<String> storing JSON objects (one object per record) and applies the given
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonRDD(json: JavaRDD[String], schema: StructType): DataFrame = {
    read.schema(schema).json(json)
  }

  /**
   * Loads an RDD[String] storing JSON objects (one object per record) inferring the
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonRDD(json: RDD[String], samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(json)
  }

  /**
   * Loads a JavaRDD[String] storing JSON objects (one object per record) inferring the
   * schema, returning the result as a [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().json()`.
   */
  @deprecated("Use read.json()", "1.4.0")
  def jsonRDD(json: JavaRDD[String], samplingRatio: Double): DataFrame = {
    read.option("samplingRatio", samplingRatio.toString).json(json)
  }

  /**
   * Returns the dataset stored at path as a DataFrame,
   * using the default data source configured by spark.sql.sources.default.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().load(path)`.
   */
  @deprecated("Use read.load(path)", "1.4.0")
  def load(path: String): DataFrame = {
    read.load(path)
  }

  /**
   * Returns the dataset stored at path as a DataFrame, using the given data source.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().format(source).load(path)`.
   */
  @deprecated("Use read.format(source).load(path)", "1.4.0")
  def load(path: String, source: String): DataFrame = {
    read.format(source).load(path)
  }

  /**
   * (Java-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().format(source).options(options).load()`.
   */
  @deprecated("Use read.format(source).options(options).load()", "1.4.0")
  def load(source: String, options: java.util.Map[String, String]): DataFrame = {
    read.options(options).format(source).load()
  }

  /**
   * (Scala-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by `read().format(source).options(options).load()`.
   */
  @deprecated("Use read.format(source).options(options).load()", "1.4.0")
  def load(source: String, options: Map[String, String]): DataFrame = {
    read.options(options).format(source).load()
  }

  /**
   * (Java-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame, using the given schema as the schema of the DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by
   *            `read().format(source).schema(schema).options(options).load()`.
   */
  @deprecated("Use read.format(source).schema(schema).options(options).load()", "1.4.0")
  def load(source: String, schema: StructType, options: java.util.Map[String, String]): DataFrame =
  {
    read.format(source).schema(schema).options(options).load()
  }

  /**
   * (Scala-specific) Returns the dataset specified by the given data source and
   * a set of options as a DataFrame, using the given schema as the schema of the DataFrame.
   *
   * @group genericdata
   * @deprecated As of 1.4.0, replaced by
   *            `read().format(source).schema(schema).options(options).load()`.
   */
  @deprecated("Use read.format(source).schema(schema).options(options).load()", "1.4.0")
  def load(source: String, schema: StructType, options: Map[String, String]): DataFrame = {
    read.format(source).schema(schema).options(options).load()
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table.
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`.
   */
  @deprecated("use read.jdbc()", "1.4.0")
  def jdbc(url: String, table: String): DataFrame = {
    read.jdbc(url, table, new Properties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table.  Partitions of the table will be retrieved in parallel based on the parameters
   * passed to this function.
   *
   * @param columnName the name of a column of integral type that will be used for partitioning.
   * @param lowerBound the minimum value of `columnName` used to decide partition stride
   * @param upperBound the maximum value of `columnName` used to decide partition stride
   * @param numPartitions the number of partitions.  the range `minValue`-`maxValue` will be split
   *                      evenly into this many partitions
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`.
   */
  @deprecated("use read.jdbc()", "1.4.0")
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int): DataFrame = {
    read.jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, new Properties)
  }

  /**
   * Construct a [[DataFrame]] representing the database table accessible via JDBC URL
   * url named table. The theParts parameter gives a list expressions
   * suitable for inclusion in WHERE clauses; each one defines one partition
   * of the [[DataFrame]].
   *
   * @group specificdata
   * @deprecated As of 1.4.0, replaced by `read().jdbc()`.
   */
  @deprecated("use read.jdbc()", "1.4.0")
  def jdbc(url: String, table: String, theParts: Array[String]): DataFrame = {
    read.jdbc(url, table, theParts, new Properties)
  }

  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////
  // End of deprecated methods
  ////////////////////////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////////////////////////


  // Register a succesfully instantiatd context to the singleton. This should be at the end of
  // the class definition so that the singleton is updated only if there is no exception in the
  // construction of the instance.
  SQLContext.setLastInstantiatedContext(self)
}

/**
 * This SQLContext object contains utility functions to create a singleton SQLContext instance,
 * or to get the last created SQLContext instance.
 */
object SQLContext {

  private val INSTANTIATION_LOCK = new Object()

  /**
   * Reference to the last created SQLContext.
   */
  @transient private val lastInstantiatedContext = new AtomicReference[SQLContext]()

  /**
   * Get the singleton SQLContext if it exists or create a new one using the given SparkContext.
   * This function can be used to create a singleton SQLContext object that can be shared across
   * the JVM.
   */
  def getOrCreate(sparkContext: SparkContext): SQLContext = {
    INSTANTIATION_LOCK.synchronized {
      if (lastInstantiatedContext.get() == null) {
        new SQLContext(sparkContext)
      }
    }
    lastInstantiatedContext.get()
  }

  private[sql] def clearLastInstantiatedContext(): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(null)
    }
  }

  private[sql] def setLastInstantiatedContext(sqlContext: SQLContext): Unit = {
    INSTANTIATION_LOCK.synchronized {
      lastInstantiatedContext.set(sqlContext)
    }
  }

  //zengdan
  def projectLoadedData(backupRdd: RDD[InternalRow], rdd: RDD[InternalRow], schema: RDD[InternalRow], output: Seq[Attribute]):RDD[InternalRow] = {
    val loadSchemaRdd = schema.collect()
    require(loadSchemaRdd.size == 1, "Length of schema is not 1!")
    require(loadSchemaRdd(0).numFields == output.size, "Length of schema doesn't match!")

    val schemaString: ArrayBuffer[String] = new ArrayBuffer[String](loadSchemaRdd(0).numFields)

    var i = 0
    while (i < loadSchemaRdd(0).numFields) {
      schemaString += loadSchemaRdd(0).getString(i)
      //schemaIndexMap += (i -> schemaWithIndex(loadSchemaRdd(0).getString(i)))
      i += 1
    }
    projectLoadedData(backupRdd, rdd, schemaString.toSeq, output)

  }

  //zengdan
  def projectLoadedData(backupRdd: RDD[InternalRow], rdd: RDD[InternalRow], schema: Seq[String], output: Seq[Attribute]):RDD[InternalRow] = {
    if (schema.isEmpty) {
      //rdd.reusePartitions(backupRdd, iter => iter)
      rdd
    } else {

      val schemaWithIndex = scala.collection.mutable.Map[String, Int]()
      //schemaWithIndex ++= output.map(_.treeStringByName).zipWithIndex

      var i = 0
      while (i < schema.size) {
        schemaWithIndex += (schema(i) -> i)
        //schemaIndexMap += (i -> schemaWithIndex(loadSchemaRdd(0).getString(i)))
        i += 1
      }

      /*
    val unsafeProjectList = projectList.map(_ transform {
      case CreateStruct(children) => CreateStructUnsafe(children)
      case CreateNamedStruct(children) => CreateNamedStructUnsafe(children)
    })
    */

      //rdd.reusePartitions(backupRdd, { loadIter =>
      rdd.mapPartitions(loadIter =>
        new Iterator[InternalRow] {
          private val mutableRow = new GenericMutableRow(output.size)
          private val converters = output.map(_.dataType).map(
            CatalystTypeConverters.createToCatalystConverter)
          val convertToUnsafe = UnsafeProjection.create(StructType.fromAttributes(output))

          override def hasNext = loadIter.hasNext

          override def next: InternalRow = {
            val input = loadIter.next()
            var i = 0
            while (i < output.size) {
              val inputIndex = try {
                schemaWithIndex.get(output(i).name).get
              } catch {
                case e: Exception =>
                  throw new RuntimeException(e)
              }
              //val inputIndex = schemaIndexMap(i)
              mutableRow(i) = converters(i)(output(i).dataType match {
                case IntegerType => input.getInt(inputIndex)
                case BooleanType => input.getBoolean(inputIndex)
                case LongType => input.getLong(inputIndex)
                case DoubleType => input.getDouble(inputIndex)
                case FloatType => input.getFloat(inputIndex)
                case ShortType => input.getShort(inputIndex)
                case ByteType => input.getByte(inputIndex)
                case StringType => input.getString(inputIndex)
                case ArrayType(_, _) => input.getArray(inputIndex)
              })
              i += 1
            }
            convertToUnsafe(mutableRow)
          }
        }
      )
    }
  }
}