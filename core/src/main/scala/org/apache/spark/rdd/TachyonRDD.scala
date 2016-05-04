package org.apache.spark.rdd

import java.util.List

import org.apache.spark.storage.{StorageLevel, RDDBlockId}
import org.apache.spark.{TaskContext, Partition, SparkEnv, SparkContext}
import tachyon.thrift.ClientFileInfo

import scala.reflect.ClassTag

/**
 * Created by zengdan on 15-10-15.
 */
private[spark] class TachyonPartition(idx: Int)
  extends Partition {
  override val index: Int = idx
}

class TachyonRDD [T: ClassTag](sc: SparkContext, operatorId: Int)
  extends ExternalStoreRDD[T](sc, operatorId){

  //var backupRDD: RDD[T] = _
  //var needRecompute = false
  //this.persist(StorageLevel.OFF_HEAP)

  //先调用getPartitions，再调用compute
  override def getPartitions: Array[Partition] = {
    val files: List[ClientFileInfo] = externalBlockStore.listStatus(operatorId)
    val ps = new Array[Partition](files.size())
    var i = 0
    while (i < files.size()) {
      val index = files.get(i).name.split("_").last.toInt
      if (files.get(i).isIsComplete) {
        ps(index) = new TachyonPartition(index)
      } else {
        ps(index) = null
      }
      i += 1
    }
    ps
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    import scala.collection.JavaConversions.asScalaBuffer
    externalBlockStore.getLocations(operatorId, split.index).toSeq
  }

  override def compute(split: Partition, context: TaskContext) = {
    val fileName = "operator_" + operatorId + "_" + split.index
    logInfo("Input Split: " + fileName)
    externalBlockStore.loadValues(new RDDBlockId(operatorId, split.index, Some(operatorId)))
      .getOrElse(Iterator.empty).asInstanceOf[Iterator[T]]
  }
}

/*
class TachyonRDD [T: ClassTag](sc: SparkContext, operatorId: Int, backupRDD: RDD[T])
  extends ExternalStoreRDD[T](sc, operatorId){

  //var backupRDD: RDD[T] = _
  //var needRecompute = false
  //this.persist(StorageLevel.OFF_HEAP)

  override def getPartitions: Array[Partition] = {
    val files: List[ClientFileInfo] = externalBlockStore.listStatus(operatorId)
    val ps = backupRDD.partitions
    var i = 0
    while (i < files.size()) {
      val index = files.get(i).name.split("_").last.toInt
      if (files.get(i).isIsComplete) {
        ps(index) = new TachyonPartition(index)
      }
      i += 1
    }
    ps
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    import scala.collection.JavaConversions.asScalaBuffer
    if (split.isInstanceOf[TachyonPartition]) {
      externalBlockStore.getLocations(operatorId, split.index).toSeq
    } else {
      backupRDD.preferredLocations(split)
    }
  }

  override def compute(split: Partition, context: TaskContext) = {
    val fileName = "operator_" + operatorId + "_" + split.index
    logInfo("Input Split: " + fileName)
    if (split.isInstanceOf[TachyonPartition]) {
      externalBlockStore.loadValues(new RDDBlockId(operatorId, split.index, Some(operatorId)))
        .getOrElse(Iterator.empty).asInstanceOf[Iterator[T]]
    } else {
      backupRDD.compute(split, context)
    }
  }
}
*/
