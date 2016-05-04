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

package org.apache.spark.storage

import java.io.IOException
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{List, Date, Random}

import tachyon.thrift.ClientFileInfo

import scala.util.control.NonFatal

import com.google.common.io.ByteStreams

import tachyon.client.{ReadType, WriteType, TachyonFS, TachyonFile}
import tachyon.conf.TachyonConf
import tachyon.TachyonURI

import org.apache.spark.{SparkConf, SparkEnv, Logging}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}


/**
 * Creates and maintains the logical mapping between logical blocks and tachyon fs locations. By
 * default, one block is mapped to one file with a name given by its BlockId.
 *
 */
private[spark] class TachyonBlockManager() extends ExternalBlockManager with Logging {
  /*
  //zengdan
  def checkGlobalExists(blockId: BlockId): Boolean = {
    val index = blockId.name.lastIndexOf('_')
    val filename = blockId.name.substring(0, index)
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % tachyonGlobalDirs.length
    val subDirId = (hash / tachyonGlobalDirs.length) % subDirsPerTachyonDir

    // Create the subdirectory if it doesn't already exist
    var subDir = subGlobalDirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subGlobalDirs(dirId).synchronized {
        val old = subGlobalDirs(dirId)(subDirId)
        if (old != null) {
          old
        } else {
          val path = new TachyonURI(tachyonGlobalDirs(dirId) + "/" + "%02x".format(subDirId))
          client.mkdir(path)
          val newDir = client.getFile(path)
          subGlobalDirs(dirId)(subDirId) = newDir
          newDir
        }
      }
    }
    val filePath = new TachyonURI(subDir + "/" + filename)
    val result = client.exist(filePath)
    result
  }
  */

  //zengdan
  def checkGlobalExists(operatorID: Int): Boolean = {
    val root = SparkEnv.get.conf.get("spark.tachyonStore.global.baseDir", "/global_spark_tachyon")
    val filePath = new TachyonURI(s"$root/${operatorID}")
    val ext = client.exist(filePath)
    ext
  }

  //zengdan
  def removeGlobalFiles(operatorID: Int) = {
    val root = SparkEnv.get.conf.get("spark.tachyonStore.global.baseDir", "/global_spark_tachyon")
    val filePath = new TachyonURI(s"$root/${operatorID}")
    val metaPath = new TachyonURI(filePath + "_meta")
    if(client.exist(filePath)){
      client.delete(filePath, true)
    }
    if(client.exist(metaPath)){
      client.delete(metaPath, true)
    }
  }

  //zengdan
  def listStatus(operatorID: Int): List[ClientFileInfo] = {
    val root = SparkEnv.get.conf.get("spark.tachyonStore.global.baseDir", "/global_spark_tachyon")
    val filePath = new TachyonURI(s"$root/${operatorID}")
    client.listStatus(filePath)
  }

  //zengdan
  def getLocations(operatorID: Int, index: Int): List[String] = {
    val root = SparkEnv.get.conf.get("spark.tachyonStore.global.baseDir", "/global_spark_tachyon")
    val filePath = new TachyonURI(s"$root/${operatorID}/operator_${operatorID}_${index}")
    client.getFile(filePath).getLocationHosts
  }

  //zengdan
  private lazy val tachyonGlobalDirs:Array[TachyonFile] = {
    val path = new TachyonURI(blockManager.conf.get("spark.tachyonStore.global.baseDir",
      "/global_spark_tachyon"))
    var foundLocalDir = false
    var tachyonDir: TachyonFile = null
    var tries = 0
    while (!foundLocalDir && tries < ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS) {
      tries += 1
      try {
        if (!client.exist(path)) {
          foundLocalDir = client.mkdir(path)
          tachyonDir = client.getFile(path)
        }else{
          foundLocalDir = true
          tachyonDir = client.getFile(path)
        }
      } catch {
        case e: Exception =>
          logWarning("Attempt " + tries + " to create tachyon dir " + tachyonDir + " failed", e)
      }
    }
    Array(tachyonDir)
  }

  //zengdan
  private lazy val subGlobalDirs = Array.fill(tachyonGlobalDirs.length)(new Array[TachyonFile](subDirsPerTachyonDir))

  var rootDirs: String = _
  var master: String = _
  var client: tachyon.client.TachyonFS = _
  private var subDirsPerTachyonDir: Int = _

  // Create one Tachyon directory for each path mentioned in spark.tachyonStore.folderName;
  // then, inside this directory, create multiple subdirectories that we will hash files into,
  // in order to avoid having really large inodes at the top level in Tachyon.
  private var tachyonDirs: Array[TachyonFile] = _
  private var subDirs: Array[Array[tachyon.client.TachyonFile]] = _


  override def init(blockManager: BlockManager, executorId: String): Unit = {
    super.init(blockManager, executorId)
    val storeDir = blockManager.conf.get(ExternalBlockStore.BASE_DIR, "/tmp_spark_tachyon")
    val appFolderName = blockManager.conf.get(ExternalBlockStore.FOLD_NAME)

    rootDirs = s"$storeDir/$appFolderName/$executorId"
    master = blockManager.conf.get(ExternalBlockStore.MASTER_URL, "tachyon://localhost:19998")
    client = if (master != null && master != "") {
      TachyonFS.get(new TachyonURI(master), new TachyonConf())
    } else {
      null
    }
    // original implementation call System.exit, we change it to run without extblkstore support
    if (client == null) {
      logError("Failed to connect to the Tachyon as the master address is not configured")
      throw new IOException("Failed to connect to the Tachyon as the master " +
        "address is not configured")
    }
    subDirsPerTachyonDir = blockManager.conf.get("spark.externalBlockStore.subDirectories",
      ExternalBlockStore.SUB_DIRS_PER_DIR).toInt

    // Create one Tachyon directory for each path mentioned in spark.tachyonStore.folderName;
    // then, inside this directory, create multiple subdirectories that we will hash files into,
    // in order to avoid having really large inodes at the top level in Tachyon.
    tachyonDirs = createTachyonDirs()
    subDirs = Array.fill(tachyonDirs.length)(new Array[TachyonFile](subDirsPerTachyonDir))
    tachyonDirs.foreach(tachyonDir => ShutdownHookManager.registerShutdownDeleteDir(tachyonDir))
  }

  override def toString: String = {"ExternalBlockStore-Tachyon"}

  override def removeBlock(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    if (fileExists(file)) {
      removeFile(file)
    } else {
      false
    }
  }

  override def blockExists(blockId: BlockId): Boolean = {
    val file = getFile(blockId)
    fileExists(file)
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val file = getFile(blockId)
    val os = file.getOutStream(WriteType.TRY_CACHE)

    //zengdan
    /*
    val writeType = SparkEnv.get.conf.get("spark.storage.tachyon.write.type", "CACHE_THROUGH")
    val os = writeType match {
      case "TRY_CACHE" => file.getOutStream(WriteType.TRY_CACHE)
      case "MUST_CACHE" => file.getOutStream(WriteType.MUST_CACHE)
      case "CACHE_THROUGH" => file.getOutStream(WriteType.CACHE_THROUGH)
      case "THROUGH" => file.getOutStream(WriteType.THROUGH)
      case "ASYNC_THROUGH" => file.getOutStream(WriteType.ASYNC_THROUGH)
    }
    */

    try {
      os.write(bytes.array())
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put bytes of block $blockId into Tachyon", e)
        os.cancel()
    } finally {
      os.close()
    }
  }

  override def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val file = getFile(blockId)
    val os = file.getOutStream(WriteType.TRY_CACHE)
    try {
      blockManager.dataSerializeStream(blockId, os, values)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into Tachyon", e)
        os.cancel()
    } finally {
      os.close()
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = getFile(blockId)
    if (file == null || file.getLocationHosts.size == 0) {
      return None
    }
    val is = file.getInStream(ReadType.CACHE)
    try {
      val size = file.length
      val bs = new Array[Byte](size.asInstanceOf[Int])
      ByteStreams.readFully(is, bs)
      Some(ByteBuffer.wrap(bs))
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get bytes of block $blockId from Tachyon", e)
        None
    } finally {
      is.close()
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val file = getFile(blockId)
    if (file == null || file.getLocationHosts().size() == 0) {
      return None
    }
    ///*
    //old version
    val is = file.getInStream(ReadType.CACHE)
    Option(is).map { is =>
      blockManager.dataDeserializeStream(blockId, is)
    }
    //*/
    /*
    //zengdan
    if(file.isInMemory) {
      val is = file.getInStream(ReadType.CACHE)
      Option(is).map { is =>
        blockManager.dataDeserializeStream(blockId, is)
      }
    }else{
      //recalculate the benefit => how??  communicate with the master
      //return whether to store it in memory
      if(true){
        //first read it from disk
        val is = file.getInStream(ReadType.CACHE)
        val values = Option(is).map { is =>
          blockManager.dataDeserializeStream(blockId, is)
        }

        //then write it to memory
        val os = file.getOutStream(WriteType.TRY_CACHE)
        try {
          blockManager.dataSerializeStream(blockId, os, values.get)
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to put values of block $blockId into Tachyon", e)
            os.cancel()
        } finally {
          os.close()
        }
        values
      }else{
        //need to modify, read from disk directly
        val is = file.getInStream(ReadType.CACHE)
        val values = Option(is).map { is =>
          blockManager.dataDeserializeStream(blockId, is)
        }
        values
      }
    }
    */
  }

  override def getSize(blockId: BlockId): Long = {
    getFile(blockId.name).length
  }

  def removeFile(file: TachyonFile): Boolean = {
    client.delete(new TachyonURI(file.getPath()), false)
  }

  def fileExists(file: TachyonFile): Boolean = {
    client.exist(new TachyonURI(file.getPath()))
  }

  //zengdan
  def getFile(filename:String, dirs: Array[TachyonFile], subs: Array[Array[TachyonFile]]): TachyonFile = {
    //filename is identified by plan.id_partition.id
    //return subs(dirId)(subDirId)/filename
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % dirs.length
    val subDirId = (hash / dirs.length) % subDirsPerTachyonDir

    // Create the subdirectory if it doesn't already exist
    var subDir = subs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subs(dirId).synchronized {
        val old = subs(dirId)(subDirId)
        if (old != null) {
          old
        } else {
          val path = new TachyonURI(dirs(dirId) + "/" + "%02x".format(subDirId))
          client.mkdir(path)
          val newDir = client.getFile(path)
          subs(dirId)(subDirId) = newDir
          newDir
        }
      }
    }
    val filePath = new TachyonURI(subDir + "/" + filename)
    if(!client.exist(filePath)) {
      client.createFile(filePath)
    }
    val file = client.getFile(filePath)
    file
  }

  //zengdan
  def getFile(filename:String, dirs: Array[TachyonFile]): TachyonFile = {
    //filename is identified by plan.id_partition.id
    //return subs(dirId)(subDirId)/filename
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % dirs.length

    val operatorId = filename.split("_")(1)

    val parentPath = new TachyonURI(dirs(dirId).getPath + "/" + operatorId)
    if(!client.exist(parentPath)){
      client.mkdir(parentPath)
    }


    val filePath = new TachyonURI(parentPath + "/" + filename)
    if(!client.exist(filePath)) {
      client.createFile(filePath)
    }
    val file = client.getFile(filePath)

    file
  }

  def getFile(filename: String): TachyonFile = {
    //zengdan
    // Figure out which tachyon directory it hashes to, and which subdirectory in that
    filename.split("_")(0) match{

      case "operator" =>
        getFile(filename, tachyonGlobalDirs)
      case _ =>
        getFile(filename, tachyonDirs, subDirs)  //original  zengdan
    }
    /*
    // Figure out which tachyon directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % tachyonDirs.length
    val subDirId = (hash / tachyonDirs.length) % subDirsPerTachyonDir

    // Create the subdirectory if it doesn't already exist
    var subDir = subDirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subDirs(dirId).synchronized {
        val old = subDirs(dirId)(subDirId)
        if (old != null) {
          old
        } else {
          val path = new TachyonURI(s"${tachyonDirs(dirId)}/${"%02x".format(subDirId)}")
          client.mkdir(path)
          val newDir = client.getFile(path)
          subDirs(dirId)(subDirId) = newDir
          newDir
        }
      }
    }
    val filePath = new TachyonURI(s"$subDir/$filename")
    if(!client.exist(filePath)) {
      client.createFile(filePath)
    }
    val file = client.getFile(filePath)
    file
    */
  }

  def getFile(blockId: BlockId): TachyonFile = getFile(blockId.name)

  // TODO: Some of the logic here could be consolidated/de-duplicated with that in the DiskStore.
  private def createTachyonDirs(): Array[TachyonFile] = {
    logDebug("Creating tachyon directories at root dirs '" + rootDirs + "'")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").map { rootDir =>
      var foundLocalDir = false
      var tachyonDir: TachyonFile = null
      var tachyonDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          tachyonDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          val path = new TachyonURI(s"$rootDir/spark-tachyon-$tachyonDirId")
          if (!client.exist(path)) {
            foundLocalDir = client.mkdir(path)
            tachyonDir = client.getFile(path)
          }
        } catch {
          case NonFatal(e) =>
            logWarning("Attempt " + tries + " to create tachyon dir " + tachyonDir + " failed", e)
        }
      }
      if (!foundLocalDir) {
        logError("Failed " + ExternalBlockStore.MAX_DIR_CREATION_ATTEMPTS
          + " attempts to create tachyon dir in " + rootDir)
        System.exit(ExecutorExitCode.EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR)
      }
      logInfo("Created tachyon directory at " + tachyonDir)
      tachyonDir
    }
  }

  override def shutdown() {
    logDebug("Shutdown hook called")
    tachyonDirs.foreach { tachyonDir =>
      try {
        if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(tachyonDir)) {
          Utils.deleteRecursively(tachyonDir, client)
        }
      } catch {
        case NonFatal(e) =>
          logError("Exception while deleting tachyon spark dir: " + tachyonDir, e)
      }
    }
    client.close()
  }
}

//zengdan
object TachyonBlockManager extends Logging{
  val conf = new SparkConf()
  val master = conf.get("spark.tachyonStore.url",  "tachyon://localhost:19998")
  val client = if (master != null && master != "") TachyonFS.get(master) else null

  if (client == null) {
    logError("Failed to connect to the Tachyon as the master address is not configured")
    System.exit(ExecutorExitCode.EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE)
  }

  val root = new TachyonURI(conf.get("spark.tachyonStore.global.baseDir",
    "/global_spark_tachyon"))


  def removeGlobalDir(conf: SparkConf) {
    logInfo("Remove global dir of tachyon storage")

    if(client.exist(root)) {
      val file = client.getFile(root)
      try {
        Utils.deleteRecursively(file, client)
      } catch {
        case e: Exception =>
          logError("Exception while deleting tachyon spark dir: " + file, e)
      }
    }

  }


  def checkOperatorFileExists(id: Int): Boolean = {
    val path = new TachyonURI(root + "/" + id)
    client.exist(path)
  }

  def ensureOperatorFileComplete(id: Int): Boolean = {
    val path = new TachyonURI(root + "/" + id)
    if (client.exist(path)) {
      val partitionFiles = client.listStatus(path)
      var i = 0
      var isComplete = true
      while (isComplete && i < partitionFiles.size()) {
        if (!partitionFiles.get(i).isComplete) {
          isComplete = false
        }
        i += 1
      }
      if (!isComplete) {
        while (client.getFileStatus(client.getFileId(path), false).isPinned) {
        }
        client.delete(path, true)
        false
      } else {
        true
      }
    } else {
      false
    }
  }

  def canReuseFile(id: Int): Boolean = {
    val path = new TachyonURI(root + "/" + id)
    if (client.exist(path)) {
      val partitionFiles = client.listStatus(path)
      var i = 0
      var isComplete = true
      while (isComplete && i < partitionFiles.size()) {
        if (!partitionFiles.get(i).isComplete) {
          isComplete = false
        }
        i += 1
      }
      if (!isComplete) {
        while (client.getFileStatus(client.getFileId(path), false).isPinned) {  //只有两个删的地方
        }
        client.delete(path, true)
        false
      } else {
        client.pinFile(client.getFileId(path))
        true
      }
    } else {
      false
    }
  }

  def unPinFile(id: Int) = {
    val path = new TachyonURI(root + "/" + id)
    if (client.exist(path)) {
      client.unpinFile(client.getFileId(path))
    }
  }
}

