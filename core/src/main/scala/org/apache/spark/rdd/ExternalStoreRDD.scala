package org.apache.spark.rdd

import java.util.List

import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{TaskContext, Partition, SparkEnv, SparkContext}
import tachyon.thrift.ClientFileInfo

import scala.reflect.ClassTag

/**
 * Created by zengdan on 15-10-15.
 */

abstract class ExternalStoreRDD [T: ClassTag](sc: SparkContext, operatorId: Int)
  extends RDD[T](sc, Nil){
  @transient lazy val externalBlockStore = SparkEnv.get.blockManager.externalBlockStore
}
