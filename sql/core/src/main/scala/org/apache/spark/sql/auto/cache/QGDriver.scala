package org.apache.spark.sql.auto.cache

/**
 * Created by zengdan on 15-3-23.
 */

import java.io.{DataOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import java.util
import java.util.HashMap
import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.actor.ActorRef
import akka.remote.{DisassociatedEvent, AssociatedEvent, RemotingLifecycleEvent}
import org.apache.spark.SparkEnv._
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler.Task
import org.apache.spark._
import org.apache.spark.serializer.{Serializer, SerializerInstance}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.auto.cache.QGMaster
import org.apache.spark.sql.auto.cache.QGMasterMessages.CacheFailed
import org.apache.spark.sql.auto.cache.QGMasterMessages.GetSchema
import org.apache.spark.sql.auto.cache.QGMasterMessages.MatchSerializedPlan
import org.apache.spark.sql.auto.cache.QGMasterMessages.SaveSchema
import org.apache.spark.sql.auto.cache.QGMasterMessages.UpdateInfo
import org.apache.spark.sql.auto.cache.QGUtils.PlanDesc
import org.apache.spark.sql.auto.cache.QGUtils.PlanUpdate
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.storage.GetBenefit
import tachyon.thrift.BenefitInfo

//import org.apache.spark.sql.catalyst.plans.physical.
import org.apache.spark.sql.auto.cache.QGMasterMessages._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.storage.BlockManagerMessages.{UpdateBlockInfo, RemoveRdd}
import org.apache.spark.util.{SerializableBuffer, Utils, SignalLogger, AkkaUtils}
import scala.collection.mutable.Map
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.auto.cache.QGUtils.{PlanDesc, PlanUpdate}


class QGDriver(sc: SparkContext) extends Actor with Logging{


  //public
  private var qgmaster: ActorRef = _
  private val conf = sc.getConf

  override def preStart() = {
    val timeout = AkkaUtils.lookupTimeout(conf)
    qgmaster = Await.result(context.actorSelection(QGMaster.toAkkaUrl).resolveOne(timeout), timeout)
    logInfo(s"Actor address in QGDriver is ${qgmaster}")
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  override def receive = {
    case MatchSerializedPlan(planDesc) =>
      logInfo("Got serialized plan in QGDriver")
      sender ! rewritePlan(planDesc)

    case UpdateInfo(cachedNode, reusedNode, stats) =>
      logInfo("Update Statistics in QGDriver")
      sender ! updateStats(cachedNode, reusedNode, stats)

    case SaveSchema(output, id) =>
      logInfo("Send Schema to QGMaster")
      sender ! saveSchema(output, id)

    case GetSchema(id) =>
      logInfo("Got schema request in QGDriver")
      sender ! getSchema(id)

    case GetBenefit(id) =>
      logInfo("Got Benefit in QGDriver")
      sender ! getBenefit(id)

    case CacheFailed(id) =>
      logInfo("Cache Failed in QGDriver")
      sender ! cacheFailed(id)

    case AssociatedEvent(localAddress, remoteAddress, inbound) =>
      logInfo(s"Successfully connected to $remoteAddress")

    case DisassociatedEvent(_, address, _) => {
      logInfo(s"$address got disassociated.")

    }
  }

  /*
  def rewritePlan(planDesc: PlanDesc): HashMap[Int, QNodeRef] = {
    askMasterWithReply[HashMap[Int, QNodeRef]](MatchSerializedPlan(planDesc))
  }
  */
  ///*
  def rewritePlan(planDesc: PlanDesc): PlanUpdate = {
    askMasterWithReply[PlanUpdate](MatchSerializedPlan(planDesc))
  }
  //*/

  def saveSchema(output: Seq[Attribute], id: Int): Boolean = {
    askMasterWithReply[Boolean](SaveSchema(output, id))
  }

  def getSchema(id: Int): Seq[Attribute] = {
    askMasterWithReply[Seq[Attribute]](GetSchema(id))
  }

  def getBenefit(id: Int):BenefitInfo = {
    askMasterWithReply[BenefitInfo](GetBenefit(id))
  }

  def cacheFailed(id: Int):Boolean = {
    askMasterWithReply[Boolean](CacheFailed(id))
  }

  def updateStats(cachedNode: Array[Int], reusedNode: Array[Int], stats: Map[Int, Array[Long]]) = {
    askMasterWithReply[Boolean](UpdateInfo(cachedNode, reusedNode, stats))
  }

  private def askMasterWithReply[T](message: Any): T = {
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[T](message, qgmaster,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
  }

}

object QGDriver{
  def createActor(sc: SparkContext): (ActorSystem, ActorRef) = {
    val conf = sc.getConf
    val securityMgr = new SecurityManager(conf)
    val hostname = conf.get("spark.driver.host", Utils.localHostName())
    val (actorSystem, _) = AkkaUtils.createActorSystem("sqlDriver", hostname, 7072, conf,
      securityMgr)
    (actorSystem, actorSystem.actorOf(Props(new QGDriver(sc)), "QGDriver"))
  }

  def rewrittenPlan(plan: SparkPlan, sqlContext: SQLContext, actor: ActorRef): PlanUpdate = {
    //QueryGraph.qg.planRewritten(plan)  //test
    ///*
    val conf = sqlContext.sparkContext.getConf
    val appId = sqlContext.sparkContext.applicationId  //make subdir in qgmaster to store jars

    val serializer = sqlContext.serializer

    val planBuffer = ByteBuffer.wrap(serializer.newInstance().serialize(plan).array())
    val plandesc = new PlanDesc(appId, sqlContext.sparkContext.addedJars, new SerializableBuffer(planBuffer))

    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout", "60").toLong, "seconds")
    val message = MatchSerializedPlan(plandesc)
    //AkkaUtils.askWithReply[HashMap[Int, QNodeRef]](message, actor,
    //  AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), timeout)
    AkkaUtils.askWithReply[PlanUpdate](message, actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
    //*/
  }

  def updateStats(cachedNode: Array[Int], reusedNode: Array[Int], stats: Map[Int, Array[Long]], sqlContext: SQLContext, actor: ActorRef): Boolean = {
    val conf = sqlContext.sparkContext.getConf
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[Boolean](UpdateInfo(cachedNode, reusedNode, stats), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
  }

  def cacheFailed(operatorId: Int, actor: ActorRef) = {
    actor ! CacheFailed(operatorId)
  }

  def saveSchema(output: Seq[Attribute], id: Int, sqlContext: SQLContext, actor: ActorRef):Boolean = {
    val conf = sqlContext.sparkContext.getConf
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[Boolean](SaveSchema(output, id), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
  }

  def getSchema(id: Int, sqlContext: SQLContext, actor: ActorRef): Seq[Attribute] = {
    val conf = sqlContext.sparkContext.getConf
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[Seq[Attribute]](GetSchema(id), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
  }

  def getBenefit(id: Int, sqlContext: SQLContext, actor: ActorRef): BenefitInfo = {
    val conf = sqlContext.sparkContext.getConf
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[BenefitInfo](GetBenefit(id), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
  }

}
