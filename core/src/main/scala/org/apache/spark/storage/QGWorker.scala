package org.apache.spark.storage

import java.nio.ByteBuffer

import akka.actor.{Props, ActorSystem, ActorRef, Actor}
import akka.remote.{DisassociatedEvent, AssociatedEvent, RemotingLifecycleEvent}
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.util.{SerializableBuffer, Utils, AkkaUtils}
import org.apache.spark.{SparkConf, SecurityManager, Logging, SparkContext}
import tachyon.thrift.BenefitInfo

import scala.collection.mutable.Map
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Created by zengdan on 15-11-3.
 */
case class ChangeToMemory(id: Int)
case class GetBenefit(id: Int)

class QGWorker(conf: SparkConf) extends Actor with Logging{

  def toAkkaUrl: String = {
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

    "akka.tcp://%s@%s:%s/user/%s".format("sqlMaster", host, port, "QGMaster")
  }


  //public
  private var qgmaster: ActorRef = _

  override def preStart() = {
    val timeout = AkkaUtils.lookupTimeout(conf)
    qgmaster = Await.result(context.actorSelection(toAkkaUrl).resolveOne(timeout), timeout)
    logInfo(s"Master Actor address in QGWorker is ${qgmaster}")
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  override def receive = {
    case ChangeToMemory(id) =>
      logInfo("Got ChangeToMemory Message in QGWorker")
      sender ! changeToMemory(id)

    case GetBenefit(id) =>
      logInfo("Got GetBenefit Message in QGWorker")
      sender ! getBenefit(id)

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


  def changeToMemory(id: Int):Boolean = {
    askMasterWithReply[Boolean](ChangeToMemory(id))
  }

  def getBenefit(id: Int):BenefitInfo = {
    askMasterWithReply[BenefitInfo](GetBenefit(id))
  }


  private def askMasterWithReply[T](message: Any): T = {
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[T](message, qgmaster,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
  }

}

object QGWorker{
  def createActor(conf: SparkConf): ActorRef = {
    val securityMgr = new SecurityManager(conf)
    val hostname = Utils.localHostName()
    val (actorSystem, _) = AkkaUtils.createActorSystem("sqlWorker", hostname, 7073, conf,
      securityMgr)
    actorSystem.actorOf(Props(new QGWorker(conf)), "QGWorker")
  }


  def changeToMemory(id: Int, conf: SparkConf, actor: ActorRef): Boolean = {
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[Boolean](ChangeToMemory(id), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
  }

  def getBenefit(id: Int, conf: SparkConf, actor: ActorRef): BenefitInfo = {
    val timeout = Duration.create(conf.get("spark.sql.auto.cache.ask.timeout","60").toLong, "seconds")
    AkkaUtils.askWithReply[BenefitInfo](GetBenefit(id), actor,
      AkkaUtils.numRetries(conf), AkkaUtils.retryWaitMs(conf), new RpcTimeout(timeout, ""))
  }

}
