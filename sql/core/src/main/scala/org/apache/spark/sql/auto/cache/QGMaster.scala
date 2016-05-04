package org.apache.spark.sql.auto.cache

/**
 * Created by zengdan on 15-3-23.
 */


import java.io.{IOException, File, DataInputStream}
import java.net.URLEncoder
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{UUID, Date}

import akka.actor._
import akka.remote.{RemotingLifecycleEvent, AssociatedEvent, DisassociatedEvent}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.master.RecoveryState
import org.apache.spark.executor._
//import org.apache.spark.executor.{ChildExecutorURLClassLoader, ExecutorURLClassLoader, MutableURLClassLoader}
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.auto.cache.QGMasterMessages.CacheFailed
import org.apache.spark.sql.auto.cache.QGMasterMessages.GetSchema
import org.apache.spark.sql.auto.cache.QGMasterMessages.MatchSerializedPlan
import org.apache.spark.sql.auto.cache.QGMasterMessages.SaveSchema
import org.apache.spark.sql.auto.cache.QGMasterMessages.UpdateInfo
import org.apache.spark.storage.{GetBenefit, ChangeToMemory, TachyonBlockManager}
import org.apache.spark.sql.auto.cache.QGMasterMessages._
import org.apache.spark.sql.auto.cache.QueryGraph
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util._
import org.apache.spark._
import scala.collection.mutable.HashMap
import scala.concurrent.Await
import scala.language.postfixOps
import org.apache.spark.sql.auto.cache.QGUtils.PlanDesc

private[spark] class QGMaster(
                               host: String,
                               port: Int,
                               webUiPort: Int,
                               val securityMgr: SecurityManager)
  extends Actor with ActorLogReceive with Logging {

  import context.dispatcher   // to use Akka's scheduler.schedule()

  Utils.checkHost(host, "Expected hostname")


  final val serialVersionUID = 273157889063959800L

  //val masterMetricsSystem = MetricsSystem.createMetricsSystem("sqlMaster", conf, securityMgr)
  //val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
  //  securityMgr)

  //val webUi = new MasterWebUI(this, webUiPort)

  val masterPublicAddress = {
    val envVar = System.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }

  val masterUrl = "spark://" + host + ":" + port
  //var masterWebUiUrl: String = _


  override def preStart() {
    logInfo("Starting Spark QGmaster at " + masterUrl)
    TachyonBlockManager.removeGlobalDir(conf)
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    //QueryGraph.main(new Array[String](0));
  }

  override def preRestart(reason: Throwable, message: Option[Any]) {
    super.preRestart(reason, message) // calls postStop()!
    logError("QGMaster actor restarted due to exception", reason)
  }


  override def receiveWithLogging = {
    case MatchSerializedPlan(planDesc) => {
      logInfo("MatchPlan in QGMaster")
      updateDependencies(planDesc.appId, planDesc.jars)
      val plan = serializer.newInstance().deserialize[SparkPlan](planDesc.serializedPlan.value,
        urlClassLoader)
      val updateInfo = QueryGraph.qg.planRewritten(plan)
      sender ! updateInfo
    }

    case UpdateInfo(cachedNode, reusedNode, statistics) =>
      logInfo("Update Statistics in QGMaster")
      QueryGraph.qg.updateStatistics(cachedNode, reusedNode, statistics)
      sender ! true

    case SaveSchema(output, id) =>
      logInfo("Save Schema in QGMaster")
      QueryGraph.qg.saveSchema(output, id)
      sender ! true

    case GetSchema(id) =>
      logInfo("Got schema request in QGMaster")
      sender ! QueryGraph.qg.getSchema(id)

    case CacheFailed(id) =>
      logInfo("Cache Failed in QGMaster")
      QueryGraph.qg.cacheFailed(id)
      sender ! true

    case ChangeToMemory(id) =>
      logInfo("Got ChangeToMemory in QGMaster")
      sender ! QueryGraph.qg.changeToMemory(id)

    case GetBenefit(id) =>
      logInfo("Got GetBenefit in QGMaster")
      sender ! QueryGraph.qg.getBenefitInfo(id)

    case AssociatedEvent(localAddress, remoteAddress, inbound) =>
      logInfo(s"Successfully connected to $remoteAddress")

    case DisassociatedEvent(_, address, _) => {
      // The disconnected client could've been either a worker or an app; remove whichever it was
      logInfo(s"$address got disassociated.")

    }

    case _ =>

      logInfo("Receive other messages")
  }

  override def postStop() {

  }

  val currentJars: HashMap[String, Long] = new HashMap[String, Long]()
  val conf = new SparkConf()
  val securityManager = new SecurityManager(conf)
  var urlClassLoader = createClassLoader()

  private def getRootDir(appId: String): String = {
    var root = System.getenv("SPARK_HOME")
    if(root == null)
      root = System.getProperty("java.io.tmpdir")
    createRootDir(root, "AutoCache/" + appId).getAbsolutePath
  }

  private def createRootDir(root: String, child: String): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a root directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, child)
        if (!dir.exists() && !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }
    dir
  }

  private def updateDependencies(appId: String, newJars: HashMap[String, Long]) = {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    //synchronized {
    // Fetch missing dependencies
    for ((name, timestamp) <- newJars if currentJars.getOrElse(name, -1L) < timestamp) {
      logInfo("Fetching " + name + " with timestamp " + timestamp)
      // Fetch file with useCache mode, close cache for local mode.
      Utils.fetchFile(name, new File(getRootDir(appId)), conf,
        securityManager, hadoopConf, timestamp, useCache = true)
      currentJars(name) = timestamp
      // Add it to our class loader
      val localName = name.split("/").last
      val url = new File(getRootDir(appId), localName).toURI.toURL
      if (!urlClassLoader.getURLs.contains(url)) {
        logInfo("Adding " + url + " to class loader")
        urlClassLoader.addURL(url)
      }
    }
    //}
  }

  private def createClassLoader(): MutableURLClassLoader = {
    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }.toArray

    //new ExecutorURLClassLoader(urls, currentLoader)
    //CURRENT MODIFY
    new MutableURLClassLoader(urls, currentLoader)
  }


  def instantiateClass[T](className: String): T = {
    val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
    try {
      cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        cls.getConstructor().newInstance().asInstanceOf[T]
    }
  }

  lazy val serializer = instantiateClass[Serializer](
    conf.get("spark.sql.plan.serializer", "org.apache.spark.serializer.JavaSerializer"))


  def deserializeWithDependencies(serializedPlan: ByteBuffer)
  : (HashMap[String, Long], ByteBuffer) = {

    val in = new ByteBufferInputStream(serializedPlan)
    val dataIn = new DataInputStream(in)

    // Read task's JARs
    val planJars = new HashMap[String, Long]()
    val numJars = dataIn.readInt()
    for (i <- 0 until numJars) {
      planJars(dataIn.readUTF()) = dataIn.readLong()
    }

    // Create a sub-buffer for the rest of the data, which is the serialized Task object
    val subBuffer = serializedPlan.slice()  // ByteBufferInputStream will have read just up to task
    (planJars, subBuffer)
  }

  def instantiateClass[T](property:String, defaultName: String): T = {
    val className = QGMaster.conf.get(property, defaultName)
    val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf])
        .newInstance(QGMaster.conf)
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        cls.getConstructor().newInstance().asInstanceOf[T]
    }
  }

}

private[spark] object QGMaster extends Logging {
  val systemName = "sqlMaster"
  private val actorName = "QGMaster"
  val sparkUrlRegex = "spark://([^:]+):([0-9]+)".r
  var url = ""
  var aSystem: ActorSystem = _
  var actor: ActorRef = _
  lazy val conf = new SparkConf

  def main(argStrings: Array[String]) {
    SignalLogger.register(log)
    val args = new QGMasterArguments(argStrings, conf)
    val (actorSystem, _) = startSystemAndActor(args.host, args.port, args.webUiPort, conf)
    aSystem = actorSystem
    url = "akka.tcp://%s@%s:%s/user/%s".format(systemName, args.host, args.port, actorName)
    actorSystem.awaitTermination()
    Runtime.getRuntime.addShutdownHook(new Thread("Stop QGMaster ActorSystem"){
      override def run(){
        if(actor != null){
          aSystem.shutdown()
        }
      }

    })

  }

  /** Returns an `akka.tcp://...` URL for the Master actor given a sparkUrl `spark://host:ip`. */
  def toAkkaUrl(sparkUrl: String): String = {
    sparkUrl match {
      case sparkUrlRegex(host, port) =>
        "akka.tcp://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
      case _ =>
        throw new SparkException("Invalid master URL: " + sparkUrl)
    }
  }

  def toAkkaUrl: String = if(url != "") url else {
    var host = Utils.localHostName()
    var port = 7070

    // Check for settings in environment variables
    if (System.getenv("SPARK_QGMASTER_HOST") != null) {
      host = System.getenv("SPARK_QGMASTER_HOST")
    }
    if (System.getenv("SPARK_QGMASTER_PORT") != null) {
      port = System.getenv("SPARK_QGMASTER_PORT").toInt
    }

    if (conf.contains("spark.qgmaster.host")) {
      host = conf.get("spark.qgmaster.host")
    }

    if (conf.contains("spark.qgmaster.port")) {
      port = conf.get("spark.qgmaster.port").toInt
    }

    "akka.tcp://%s@%s:%s/user/%s".format(systemName, host, port, actorName)
  }

  def startSystemAndActor(
                           host: String,
                           port: Int,
                           webUiPort: Int,
                           conf: SparkConf): (ActorSystem, Int) = {
    val securityMgr = new SecurityManager(conf)
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem(systemName, host, port, conf = conf,
      securityManager = securityMgr)
    actor = actorSystem.actorOf(Props(classOf[QGMaster], host, boundPort, webUiPort,
      securityMgr), actorName)
    (actorSystem, boundPort)
  }
}

