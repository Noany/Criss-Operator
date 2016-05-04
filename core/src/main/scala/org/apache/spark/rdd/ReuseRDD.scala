package org.apache.spark.rdd

import java.io.EOFException
import java.text.SimpleDateFormat
import java.util.{Date, HashMap}

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapred.lib.CombineFileSplit
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.{DataReadMethod, InputMetrics}
import org.apache.spark.util.{SerializableConfiguration, ShutdownHookManager, Utils, NextIterator}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
 * Created by zengdan on 15-6-1.
 */

private[spark] class HadoopCombinePartition(rddId: Int, idx: Int, @transient s: Array[InputSplit])
  extends Partition {

  val inputSplits = new SerializableArrayWritable[InputSplit](s)

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  override val index: Int = idx

  /**
   * Get any environment variables that should be added to the users environment when running pipes
   * @return a Map with the environment variables and corresponding values, it could be empty
   */
  def getPipeEnvVars(): Map[String, String] = {
    val envVars: Map[String, String] = if (inputSplits.value.length>0 && inputSplits.value(0).isInstanceOf[FileSplit]) {
      val is: FileSplit = inputSplits.value(0).asInstanceOf[FileSplit]
      // map_input_file is deprecated in favor of mapreduce_map_input_file but set both
      // since its not removed yet
      Map("map_input_file" -> is.getPath().toString(),
        "mapreduce_map_input_file" -> is.getPath().toString())
    } else {
      Map()
    }
    envVars
  }
}

class ReuseRDD[K, V](
                      @transient sc: SparkContext,
                      broadcastedConf: Broadcast[SerializableConfiguration],
                      initLocalJobConfFuncOpt: Option[JobConf => Unit],
                      inputFormatClass: Class[_ <: InputFormat[K, V]],
                      keyClass: Class[K],
                      valueClass: Class[V],
                      minPartitions: Int)
  extends HadoopRDD[K, V](sc, broadcastedConf, initLocalJobConfFuncOpt,
    inputFormatClass, keyClass, valueClass, minPartitions) with Logging {


  override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    val inputFormat = getInputFormat(jobConf)
    if (inputFormat.isInstanceOf[Configurable]) {
      inputFormat.asInstanceOf[Configurable].setConf(jobConf)
    }

    val inputSplits = inputFormat.getSplits(jobConf, minPartitions)
    val splits = new HashMap[Int, ArrayBuffer[InputSplit]]()

    for (i <- 0 until inputSplits.size) {
      val path = inputSplits(i).asInstanceOf[FileSplit].getPath.toString
      val index = path.lastIndexOf('_')
      val partitionId = path.substring(index+1).toInt
      var partitions = splits.get(partitionId)
      if(partitions == null){
        partitions = new ArrayBuffer[InputSplit]()
      }
      partitions += (inputSplits(i))
      splits.put(partitionId, partitions)
      //array(i) = new HadoopPartition(id, path.substring(index+1).toInt, inputSplits(i))
    }
    val array = new Array[Partition](splits.size())
    for(partitionId <- 0 to splits.size() - 1){
      array(partitionId) = new HadoopCombinePartition(id, partitionId,
        splits.get(partitionId).toArray[InputSplit])
    }
    array
  }

  override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    val iter = new NextIterator[(K, V)] {

      val splits = theSplit.asInstanceOf[HadoopCombinePartition].inputSplits.value

      for(split <- splits)
        logInfo("Input split: " + split)
      val jobConf = getJobConf()

      val inputMetrics = new InputMetrics(DataReadMethod.Hadoop)
      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      val bytesReadCallback = if (splits.length > 0 && splits(0).isInstanceOf[FileSplit]) {
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
      } else {
        None
      }

      inputMetrics.setBytesReadCallback(bytesReadCallback)

      //for(inputSplit <- splits.inputSplits.value){

      var cur = 0

      var reader: RecordReader[K, V] = null
      val inputFormat = getInputFormat(jobConf)
      HadoopRDD.addLocalConfiguration(new SimpleDateFormat("yyyyMMddHHmm").format(new Date()),
        context.stageId, theSplit.index, context.attemptId.toInt, jobConf)
      reader = inputFormat.getRecordReader(splits(cur), jobConf, Reporter.NULL)

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener{ context => closeIfNeeded() }
      val key: K = reader.createKey()
      val value: V = reader.createValue()

      var recordsSinceMetricsUpdate = 0

      override def getNext() = {
        var curFinish = false
        try {
          curFinish = !reader.next(key, value)
          while(curFinish && cur < splits.length - 1){
            cur += 1
            reader.close()
            reader = inputFormat.getRecordReader(splits(cur), jobConf, Reporter.NULL)
            curFinish = !reader.next(key, value)
          }

          if(curFinish && cur == splits.length - 1)
            finished = true
        } catch {
          case eof: EOFException =>
            finished = true
        }

        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        (key, value)
      }


      override def close() {
        try {
          reader.close()
          if (bytesReadCallback.isDefined) {
            inputMetrics.updateBytesRead()
          } else {
            for(inputSplit <- splits){
              if(inputSplit.isInstanceOf[FileSplit]){
                try {
                  inputMetrics.incBytesRead(inputSplit.getLength)
                } catch {
                  case e: java.io.IOException =>
                    logWarning("Unable to get input size to set InputMetrics for task", e)
                }
              }
            }
          }
        } catch {
          case e: Exception => {

            if (!ShutdownHookManager.inShutdown()) {
              logWarning("Exception in RecordReader.close()", e)
            }
          }
        }
      }
    }
    new InterruptibleIterator[(K, V)](context, iter)
  }



  override def getPreferredLocations(split: Partition): Seq[String] = {
    val hsplits = split.asInstanceOf[HadoopCombinePartition].inputSplits.value
    val locs: Option[Seq[String]] = HadoopRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val x = hsplits.foldLeft(new ArrayBuffer[String]()) { (seqs, hsplit) =>
            val lsplit = c.inputSplitWithLocationInfo.cast(hsplit)
            val infos = c.getLocationInfo.invoke(lsplit).asInstanceOf[Array[AnyRef]]
            seqs ++= HadoopRDD.convertSplitLocationInfo(infos)
            seqs
          }
          Some(x.toSeq)
        } catch {
          case e: Exception =>
            logDebug("Failed to use InputSplitWithLocations.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(hsplits.flatMap(hsplit => hsplit.getLocations.filter(_ != "localhost")))
  }
}
