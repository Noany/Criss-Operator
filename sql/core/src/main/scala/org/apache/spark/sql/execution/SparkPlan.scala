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

package org.apache.spark.sql.execution

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Stats

import scala.collection.Iterator._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import org.apache.spark.{SparkEnv, Logging}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{TachyonRDD, RDD, RDDOperationScope}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{ScalaReflection, InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{AtomicType, DataType}

//zengdan
case class QNodeRef(var id: Int, var cache: Boolean, var collect: Boolean, var reuse: Boolean = false, var existTime: Long)


object SparkPlan {
  protected[sql] val currentContext = new ThreadLocal[SQLContext]()
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable {
  //zengdan
  protected[spark]  final val serialVersionUID = 273157889063959800L

  @volatile protected[spark] var rowCount:Int = 0
  @volatile protected[spark] var avgSize:Int = 0
  @volatile protected[spark] var time: Long = 0
  var id: Int = -1
  var nodeRef: Option[QNodeRef] = None
  var childTime = -1L
  //@transient lazy val (fixedSize, varIndexes) = outputSize(output)
  //protected[spark] var partialCollect: Boolean = false
  //val (fixedSize, varIndexes) = outputSize(output)
  var fixedSize: Int = _
  var varIndexes: List[Int] = _
  var outputAttribute: Seq[Attribute] = _


  def updateNodeRef(ref: Option[QNodeRef]) = {
    this.nodeRef = ref
    if (nodeRef.isDefined && nodeRef.get.collect) {
      outputAttribute = output
      val size = outputSize(outputAttribute)
      fixedSize = size._1
      varIndexes = size._2
    }

  }

  //zengdan
  lazy val transformedExpressions: Seq[Expression] = {
    this.expressions.foldLeft(new ArrayBuffer[Expression]){
      (buffers, expression) =>
        buffers.append(expression.transformExpression())
        buffers
    }.toSeq
  }

  //zengdan
  def operatorMatch(plan: SparkPlan):Boolean = {
    //(plan.getClass == this.getClass) && compareExpressions(plan.expressions, this.expressions)
    (plan.getClass == this.getClass) && compareExpressions(plan.transformedExpressions, this.transformedExpressions)
  }

  def compareOptionExpression(expr1: Option[Expression], expr2: Option[Expression]): Boolean = {
    if(expr1.isDefined && expr2.isDefined){
      compareExpressions(Seq(expr1.get.transformExpression()), Seq(expr2.get.transformExpression()))
    }else{
      !expr1.isDefined && !expr2.isDefined
    }
  }

  //zengdan
  def compareExpressions(expr1: Seq[Expression], expr2: Seq[Expression]): Boolean ={
    val e1 = expr1.map(_.treeStringByName).sortWith(_.compareTo(_) < 0)
    val e2 = expr2.map(_.treeStringByName).sortWith(_.compareTo(_) < 0)

    if(e1.length != e2.length)
      false
    else if(e1.length == 0)
      true
    else{
      var i = 0
      while(i < e1.length){
        if(e1(i).compareTo(e2(i)) != 0)
          return false
        i += 1
      }
      i != 0
    }
  }

  //zengdan
  protected def outputSize(schema: Seq[Attribute]):(Int, List[Int]) = {
    var fixed = 0
    var i = 0

    var varIndex: List[Int] = Nil
    while(i < schema.size){
      if(schema(i).dataType.isInstanceOf[AtomicType]){  //only support nativetype,other:array,map,struct
      var tp = schema(i).dataType.defaultSize
        if(tp == 4096) {
          //varIndex = varIndex ::: List(i)
          varIndex = i :: varIndex
        }else{
          fixed += tp
        }
      }else{
        //varIndex = varIndex ::: List(i)
        varIndex = i :: varIndex
      }
      i+=1
    }
    (fixed, varIndex)
  }

  //zengdan
  protected def cacheData(newRdd: RDD[InternalRow], output: Seq[Attribute]):RDD[InternalRow] = {
    if(nodeRef.isDefined && nodeRef.get.cache) {
      //return sqlContext.cacheData(newRdd, output, nodeRef.get.id)
      if(sqlContext.cacheData(output, nodeRef.get.id)){
        //send schema to QGMaster
        val rdd = newRdd.map(_.copy())
        rdd.cacheID = Some(nodeRef.get.id)
        rdd.persist(StorageLevel.OFF_HEAP)
        //val path = sqlContext.sparkContext.getGlobalPath(nodeRef.get.id)
        //rdd.saveAsObjectFile(path)
        //sqlContext.sparkContext.objectFile(path)]
        rdd
      }else{
        newRdd
      }
    }else {
      newRdd
    }
  }



  def mapWithReuse[T](iter: Iterator[T], f: T => InternalRow,
                      materialization: Boolean = false) = {
    val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
    if(shouldCollect){
      var start: Long = 0
      //val time = longMetric("time")
      //val size = longMetric("size")

      new Iterator[InternalRow]{
        var i = 0

        override def hasNext = {
          start = System.nanoTime()
          val has = iter.hasNext
          time += (System.nanoTime() - start)
          if (!has && rowCount != 0) {
            avgSize = (fixedSize + avgSize / rowCount)
            logDebug(s"${nodeName} ${nodeRef.get.id}: $time, $rowCount, $avgSize")
            val materializationTime = Stats.statistics.get.get(0).getOrElse(Array(0))(0)
            Stats.statistics.get.put(nodeRef.get.id,
              Array((time / 1e6).toInt + Stats.initialTimes.get.get(nodeRef.get.id).get + materializationTime, avgSize * rowCount))
            if(materialization)
              Stats.statistics.get.put(0,
                Array((time / 1e6).toInt + materializationTime, 0))

          }
          has
        }

        override def next = {
          start = System.nanoTime()
          val result = f(iter.next())
          time += (System.nanoTime() - start)
          rowCount += 1
          i = 0
          while (i < varIndexes.size) {
          //for (index <- varIndexes) {
            //sizeInBytes += result.getString(index).length
            avgSize += outputAttribute(varIndexes(i)).dataType.size(result, varIndexes(i))
            i += 1
          }

          result
        }
      }
    }else{
      iter.map(f)
    }
  }



  //zengdan
  def filterWithReuse(iter: Iterator[InternalRow], f: InternalRow => Boolean,
                       materialization: Boolean = false) = {
    val shouldCollect = nodeRef.isDefined && nodeRef.get.collect
    if(shouldCollect){
      var start: Long = 0

      new Iterator[InternalRow]{
        var hd: InternalRow = null
        var hdDefined: Boolean = false
        var flag: Boolean = true
        var i = 0

        override def hasNext: Boolean = hdDefined || {
            do {
              start = System.nanoTime()
              val has = iter.hasNext
              time += (System.nanoTime() - start)

              if (!has) {
                if (rowCount != 0) {
                  avgSize = (fixedSize + avgSize / rowCount)
                  logDebug(s"${nodeName} ${nodeRef.get.id}: $time, $rowCount, $avgSize")
                  val materializationTime = Stats.statistics.get.get(0).getOrElse(Array(0))(0)
                  Stats.statistics.get.put(nodeRef.get.id,
                    Array((time / 1e6).toInt + Stats.initialTimes.get.get(nodeRef.get.id).get + materializationTime, avgSize * rowCount))
                  if(materialization)
                    Stats.statistics.get.put(0,
                      Array((time / 1e6).toInt + materializationTime, 0))
                }
                return false
              }

              start = System.nanoTime()
              hd = iter.next()
              flag = f(hd)
              time += (System.nanoTime() - start)
            } while (!flag)
            hdDefined = true
            true
          }

          override def next() = {
            var result: InternalRow = null
            if (hasNext) {
              hdDefined = false
              result = hd
              rowCount += 1
              i = 0
              while (i < varIndexes.size) {
                //for (index <- varIndexes) {
                //sizeInBytes += result.getString(index).length
                avgSize += outputAttribute(varIndexes(i)).dataType.size(result, varIndexes(i))
                i += 1
              }
            } else
              throw new NoSuchElementException("next on empty iterator")
            result
          }
      }
    }else{
      iter.filter(f)
    }
  }


  /**
   * A handle to the SQL Context that was used to create this plan.   Since many operators need
   * access to the sqlContext for RDD operations or configuration this field is automatically
   * populated by the query planning infrastructure.
   */
  @transient
  protected[spark] final val sqlContext = SparkPlan.currentContext.get()

  protected def sparkContext = sqlContext.sparkContext

  // sqlContext will be null when we are being deserialized on the slaves.  In this instance
  // the value of codegenEnabled/unsafeEnabled will be set by the desserializer after the
  // constructor has run.
  val codegenEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.codegenEnabled
  } else {
    false
  }
  val unsafeEnabled: Boolean = if (sqlContext != null) {
    sqlContext.conf.unsafeEnabled
  } else {
    false
  }

  /**
   * Whether the "prepare" method is called.
   */
  private val prepareCalled = new AtomicBoolean(false)

  /** Overridden make copy also propogates sqlContext to copied plan. */
  override def makeCopy(newArgs: Array[AnyRef]): SparkPlan = {
    SparkPlan.currentContext.set(sqlContext)
    super.makeCopy(newArgs)
  }

  /**
   * Return all metrics containing metrics of this SparkPlan.
   */
  private[sql] def metrics: Map[String, SQLMetric[_, _]] = Map.empty

  /**
   * Return a LongSQLMetric according to the name.
   */
  private[sql] def longMetric(name: String): LongSQLMetric =
    metrics(name).asInstanceOf[LongSQLMetric]

  // TODO: Move to `DistributedPlan`
  /** Specifies how data is partitioned across different nodes in the cluster. */
  def outputPartitioning: Partitioning = UnknownPartitioning(0) // TODO: WRONG WIDTH!

  /** Specifies any partition requirements on the input data for this operator. */
  def requiredChildDistribution: Seq[Distribution] =
    Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies how data is ordered in each partition. */
  def outputOrdering: Seq[SortOrder] = Nil

  /** Specifies sort order for each partition requirements on the input data for this operator. */
  def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  /** Specifies whether this operator outputs UnsafeRows */
  def outputsUnsafeRows: Boolean = false

  /** Specifies whether this operator is capable of processing UnsafeRows */
  def canProcessUnsafeRows: Boolean = false

  /**
   * Specifies whether this operator is capable of processing Java-object-based Rows (i.e. rows
   * that are not UnsafeRows).
   */
  def canProcessSafeRows: Boolean = true

  /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to doExecute
   * after adding query plan information to created RDDs for visualization.
   * Concrete implementations of SparkPlan should override doExecute instead.
   */
  final def execute(): RDD[InternalRow] = {
    if (children.nonEmpty) {
      val hasUnsafeInputs = children.exists(_.outputsUnsafeRows)
      val hasSafeInputs = children.exists(!_.outputsUnsafeRows)
      assert(!(hasSafeInputs && hasUnsafeInputs),
        "Child operators should output rows in the same format")
      assert(canProcessSafeRows || canProcessUnsafeRows,
        "Operator must be able to process at least one row format")
      assert(!hasSafeInputs || canProcessSafeRows,
        "Operator will receive safe rows as input but cannot process safe rows")
      assert(!hasUnsafeInputs || canProcessUnsafeRows,
        "Operator will receive unsafe rows as input but cannot process unsafe rows")
    }
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      prepare()
      //zengdan original:doExecute
      //TODO: considering eager doExecute like TakeOrderedAndProject
      val original = doExecute()
      val resultRdd = reuseData(original)
      if(nodeRef.isDefined && nodeRef.get.collect)
        resultRdd.collectID = Some(nodeRef.get.id)

      //val resultRdd = reuseData().getOrElse(doExecute())
      cacheData(resultRdd, output)
      //zengdan
    }
  }

  /**
   * Prepare a SparkPlan for execution. It's idempotent.
   */
  final def prepare(): Unit = {
    if (prepareCalled.compareAndSet(false, true)) {
      doPrepare()
      children.foreach(_.prepare())
    }
  }

  //zengdan
  //reuse data
  final def reuseData(backupRdd: RDD[InternalRow]):RDD[InternalRow] = {
    sqlContext.loadData(output, nodeRef, backupRdd)
  }

  /**
   * Overridden by concrete implementations of SparkPlan. It is guaranteed to run before any
   * `execute` of SparkPlan. This is helpful if we want to set up some state before executing the
   * query, e.g., `BroadcastHashJoin` uses it to broadcast asynchronously.
   *
   * Note: the prepare method has already walked down the tree, so the implementation doesn't need
   * to call children's prepare methods.
   */
  protected def doPrepare(): Unit = {}

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  protected def doExecute(): RDD[InternalRow]

  /**
   * Runs this query returning the result as an array.
   */
  def executeCollect(): Array[Row] = {
    val finalRdd = execute().mapPartitions { iter =>
      val converter = CatalystTypeConverters.createToScalaConverter(schema)
      iter.map(converter(_).asInstanceOf[Row])
    }

    this.sparkContext.setBenefit(this.sqlContext.idToBenefit)
    val result = finalRdd.collect()
    //traverse the plan to update the time
    //zengdan
    if(this.sparkContext.getConf.get("spark.sql.auto.cache", "false").toBoolean) {
      val statistics = sparkContext.dagScheduler.getStatistics
      updateTime(this, statistics)
      auto.cache.QGDriver.updateStats(this.sqlContext.cachedNode.toArray[Int],
        this.sqlContext.reusedNode.toArray[Int],
        statistics, this.sqlContext, this.sqlContext.qgDriver)
      sparkContext.dagScheduler.clearStatistics
    }
    result
  }

  //zengdan
  def updateTime(plan: SparkPlan, statistics: mutable.Map[Int, Array[Long]]): Long ={
    if(plan.children.isEmpty) return 0
    var preStage = 0L
    for (child <- plan.children) {
      preStage += updateTime(child, statistics)
    }
    if (plan.nodeRef.isDefined) {
      val planId = plan.nodeRef.get.id
      if (plan.isInstanceOf[Exchange]) {
        if (plan.nodeRef.get.existTime == -1) {
          val exchangeStats = statistics.get(planId)
          if (exchangeStats.isDefined) {
            preStage += statistics.get((-1) * (planId)).get(0)
            statistics.remove((-1) * (planId))
          }
        } else {
          preStage += plan.nodeRef.get.existTime
        }
      }
      val stats = statistics.get(planId)
      if (stats.isDefined) {
        stats.get(0) += preStage
      }
    }
    preStage
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   *
   * This is modeled after RDD.take but never runs any job locally on the driver.
   */
  def executeTake(n: Int): Array[Row] = {
    if (n == 0) {
      return new Array[Row](0)
    }

    val childRDD = execute().map(_.copy())

    val buf = new ArrayBuffer[InternalRow]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = n - buf.size
      val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)
      val sc = sqlContext.sparkContext
      val res =
        sc.runJob(childRDD, (it: Iterator[InternalRow]) => it.take(left).toArray, p)

      res.foreach(buf ++= _.take(n - buf.size))
      partsScanned += numPartsToTry
    }

    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    buf.toArray.map(converter(_).asInstanceOf[Row])
  }

  private[this] def isTesting: Boolean = sys.props.contains("spark.testing")

  protected def newProjection(
      expressions: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    log.debug(
      s"Creating Projection: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if (codegenEnabled) {
      try {
        GenerateProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate projection, fallback to interpret", e)
            new InterpretedProjection(expressions, inputSchema)
          }
      }
    } else {
      new InterpretedProjection(expressions, inputSchema)
    }
  }

  protected def newMutableProjection(
      expressions: Seq[Expression],
      inputSchema: Seq[Attribute]): () => MutableProjection = {
    log.debug(
      s"Creating MutableProj: $expressions, inputSchema: $inputSchema, codegen:$codegenEnabled")
    if(codegenEnabled) {
      try {
        GenerateMutableProjection.generate(expressions, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate mutable projection, fallback to interpreted", e)
            () => new InterpretedMutableProjection(expressions, inputSchema)
          }
      }
    } else {
      () => new InterpretedMutableProjection(expressions, inputSchema)
    }
  }

  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): (InternalRow) => Boolean = {
    if (codegenEnabled) {
      try {
        GeneratePredicate.generate(expression, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate predicate, fallback to interpreted", e)
            InterpretedPredicate.create(expression, inputSchema)
          }
      }
    } else {
      InterpretedPredicate.create(expression, inputSchema)
    }
  }

  protected def newOrdering(
      order: Seq[SortOrder],
      inputSchema: Seq[Attribute]): Ordering[InternalRow] = {
    if (codegenEnabled) {
      try {
        GenerateOrdering.generate(order, inputSchema)
      } catch {
        case e: Exception =>
          if (isTesting) {
            throw e
          } else {
            log.error("Failed to generate ordering, fallback to interpreted", e)
            new InterpretedOrdering(order, inputSchema)
          }
      }
    } else {
      new InterpretedOrdering(order, inputSchema)
    }
  }
  /**
   * Creates a row ordering for the given schema, in natural ascending order.
   */
  protected def newNaturalAscendingOrdering(dataTypes: Seq[DataType]): Ordering[InternalRow] = {
    val order: Seq[SortOrder] = dataTypes.zipWithIndex.map {
      case (dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    newOrdering(order, Seq.empty)
  }
}

private[sql] trait LeafNode extends SparkPlan {
  override def children: Seq[SparkPlan] = Nil
}

private[sql] trait UnaryNode extends SparkPlan {
  def child: SparkPlan

  override def children: Seq[SparkPlan] = child :: Nil

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

private[sql] trait BinaryNode extends SparkPlan {
  def left: SparkPlan
  def right: SparkPlan

  override def children: Seq[SparkPlan] = Seq(left, right)
}
