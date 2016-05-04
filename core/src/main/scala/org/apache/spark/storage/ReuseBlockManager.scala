package org.apache.spark.storage

/**
 * Created by zengdan on 15-11-3.
 */

import java.io.IOException
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.{List, Date, Random}

import tachyon.thrift.{BenefitInfo, ClientFileInfo}

import scala.util.control.NonFatal

import com.google.common.io.ByteStreams

import tachyon.client._
import tachyon.conf.TachyonConf
import tachyon.TachyonURI

import org.apache.spark.{SparkConf, SparkEnv, Logging}
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.{ShutdownHookManager, Utils}



private[spark] class ReuseBlockManager() extends ExternalBlockManager with Logging {
  var idToBenefit = scala.collection.mutable.Map[Int, BenefitInfo]()


  override def setBenefit(benefit: collection.mutable.Map[Int, BenefitInfo]) = {
    idToBenefit = benefit
  }

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

  lazy val qgWorker = QGWorker.createActor(SparkEnv.get.conf)


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
  def setTachyonConf(tachyonURI: TachyonURI, conf: TachyonConf): TachyonConf = {
    require(conf != null, "Could not pass null TachyonConf instance.");
    if(tachyonURI == null) {
      throw new IOException("Tachyon Uri cannot be null. Use tachyon://host:port/ ,tachyon-ft://host:port/");
    } else {
      val scheme:String = tachyonURI.getScheme();
      if(scheme != null && tachyonURI.getHost() != null && tachyonURI.getPort() != -1 && ("tachyon".equals(scheme) || "tachyon-ft".equals(scheme))) {
        val useZookeeper:Boolean = scheme.equals("tachyon-ft");
        conf.set("tachyon.usezookeeper", "" + useZookeeper);
        conf.set("tachyon.master.hostname", tachyonURI.getHost());
        conf.set("tachyon.master.port", Integer.toString(tachyonURI.getPort()));
        return conf;
      } else {
        throw new IOException("Invalid Tachyon URI: " + tachyonURI + ". Use " + "tachyon://" + "host:port/ ," + "tachyon-ft://" + "host:port/");
      }
    }
  }

  //zengdan
  private lazy val subGlobalDirs = Array.fill(tachyonGlobalDirs.length)(new Array[TachyonFile](subDirsPerTachyonDir))

  var rootDirs: String = _
  var master: String = _
  var client: tachyon.client.TachyonFS = _
  //zengdan
  var tachyonConf: TachyonConf = _
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

    tachyonConf = if (master != null && master != "") {
      setTachyonConf(new TachyonURI(master), new TachyonConf())
    }else{
      null
    }

    // original implementation call System.exit, we change it to run without extblkstore support
    if (tachyonConf == null) {
      logError("Failed to connect to the Tachyon as the master address is not configured")
      throw new IOException("Failed to connect to the Tachyon as the master " +
        "address is not configured")
    }

    client = TachyonFS.get(tachyonConf)


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
    if (fileExists(blockId)) {
      client.delete(new TachyonURI(getFile(blockId).getPath()), false)
    } else {
      false
    }
  }

  override def blockExists(blockId: BlockId): Boolean = {
    fileExists(blockId)
  }



  override def putBytes(blockId: BlockId, bytes: ByteBuffer): Unit = {
    val file = getFile(blockId)
    val os = if (blockId.name.startsWith("operator")) {
      val fileName = blockId.name.split("_")
      val id = fileName(fileName.length - 2).toInt
      val index = fileName(fileName.length - 1).toInt
      //val os = file.getOutStream(WriteType.TRY_CACHE)
      //val benefit = QGWorker.getBenefit(id, SparkEnv.get.conf, qgWorker)
      val benefit = idToBenefit.get(id).getOrElse(QGWorker.getBenefit(id, SparkEnv.get.conf, qgWorker))
      //idToBenefit.put(id, benefit)  benefit在不断彼岸花
      //val os = file.getOutStream(WriteType.TRY_CACHE)
      var partitionOs: OutStream = null
      try {
        file.getOutStream(WriteType.TRY_CACHE, bytes.array().size, id, index, benefit)
      } catch {
        case e: IOException =>
          logWarning(s"Failed to put bytes of block into Tachyon because of getOutStream")
          logError(e.getMessage)
          return
      }
    } else {
      file.getOutStream(WriteType.TRY_CACHE)
    }

    try {
      os.write(bytes.array())
    } catch {
      case e: IOException =>
        logWarning(s"Failed to put bytes of block into Tachyon because of write")
        //logError(e.getMessage)
        os.cancel()
      case NonFatal(e) =>
        logWarning(s"Failed to put bytes of block $blockId into Tachyon", e)
        os.cancel()
    } finally {
      os.close()
    }

  }

  //zengdan
  override def loadBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = getFile(blockId)
    if (file == null || file.getLocationHosts.size == 0) {
      return None
    }

    /*
    try {
      val size = file.length
      val bs = new Array[Byte](size.asInstanceOf[Int])
      ByteStreams.readFully(is, bs)
      is.close()
      //判断是否在内存
      if (is.isInstanceOf[LocalBlockInStream] && !file.isInMemory) {
        val path = file.getPath.split("/");
        val fileName = path(path.length - 1).split("_")
        val id = fileName(fileName.length - 2).toInt
        val index = fileName(fileName.length - 1).toInt
        val changeToMemory = QGWorker.changeToMemory(id, SparkEnv.get.conf, qgWorker)
        if (changeToMemory) {
          //delete file
          client.delete(new TachyonURI(file.getPath), false);

          //store file
          val benefit = idToBenefit.get(id).getOrElse(QGWorker.getBenefit(id, SparkEnv.get.conf, qgWorker))
          idToBenefit.put(id, benefit)
          val memoryFile = getFile(blockId)
          val os = memoryFile.getOutStream(WriteType.TRY_CACHE, size, id, index, benefit)
          try {
            os.write(bs)
          } catch {
            case NonFatal(e) =>
              logWarning(s"Failed to change bytes of block $blockId into Tachyon memory", e)
              os.cancel()
          } finally {
            os.close()
          }
        }
      }
      Some(ByteBuffer.wrap(bs))
    } catch {
      case NonFatal(e) =>
        //is.close()
        logWarning(s"Failed to load bytes of block $blockId from Tachyon", e)
        None
      case _ =>
        //is.close()
        None
    }
    */

    val is = file.getInStream(ReadType.NO_CACHE)
    //只有一个block时返回blockStream，会锁住对应的block
    //有多个block时返回FileInStream，blockStream在读之前未进行初始化
    val size = file.length
    val bs = new Array[Byte](size.asInstanceOf[Int])
    ByteStreams.readFully(is, bs)
    is.close()

    try {

      //判断是否在内存
      if (!file.isInMemory) {
        val path = file.getPath.split("/");
        val fileName = path(path.length - 1).split("_")
        val id = fileName(fileName.length - 2).toInt
        val index = fileName(fileName.length - 1).toInt

        val changeToMemory = false //QGWorker.changeToMemory(id, SparkEnv.get.conf, qgWorker)
        if (changeToMemory) {
          //store file
          val benefit = idToBenefit.get(id).getOrElse(QGWorker.getBenefit(id, SparkEnv.get.conf, qgWorker))
          //val benefit = QGWorker.getBenefit(id, SparkEnv.get.conf, qgWorker)
            //idToBenefit.get(id).getOrElse(QGWorker.getBenefit(id, SparkEnv.get.conf, qgWorker))
          //idToBenefit.put(id, benefit)
          try {
            client.movePartitionToMemory(id, index, benefit, file)
          } catch {
            case NonFatal(e) =>
              logWarning(s"Failed to move block $blockId of file ${file.getPath} into Tachyon memory", e)
          }
        }
      }


      Some(ByteBuffer.wrap(bs))
    } catch {
      case NonFatal(e) =>
        is.close()
        logWarning(s"Failed to load bytes of block $blockId from Tachyon", e)
        None
      case _ =>
        is.close()
        None
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = getFile(blockId)
    if (file == null || file.getLocationHosts.size == 0) {
      return None
    }
    val is = file.getInStream(ReadType.NO_CACHE) //zengdan CACHE -> NO_CACHE
    try {
      val size = file.length
      if (size == 0) {  //zengdan
        None
      } else {
        val bs = new Array[Byte](size.asInstanceOf[Int])
        ByteStreams.readFully(is, bs)
        Some(ByteBuffer.wrap(bs))
      }
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get bytes of block $blockId from Tachyon", e)
        None
    } finally {
      is.close()
    }
  }

  /*
  def getInStream(file: TachyonFile, readType: ReadType)={
    if(readType == null) {
      throw new IOException("ReadType can not be null.");
    } else if(!file.isComplete()) {
      throw new IOException("The file " + this + " is not complete.");
    } else if(file.isDirectory()) {
      throw new IOException("Cannot open a directory for reading.");
    } else {
      val size = file.getNumberOfBlocks
      if(size == 0){
        new EmptyBlockInStream(this, readType, tachyonConf)
      }else if(size == 1){
        new UnderfsBlockInStream(file, readType, 0, SparkEnv.get.conf, tachyonConf)
      }else{
        new UnderfsFileInStream(file, readType, SparkEnv.get.conf, tachyonConf)
      }

    }
  }
  */

  /*
  override def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val file = getFile(blockId)
    if (file == null || file.getLocationHosts().size() == 0) {
      return None
    }
    val is = file.getInStream(ReadType.NO_CACHE) //zengdan
    Option(is).map { is =>
      blockManager.dataDeserializeStream(blockId, is)
    }
  }
  */

  override def getSize(blockId: BlockId): Long = {
    getFile(blockId.name).length
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
          val path = new TachyonURI(dirs(dirId).getPath + "/" + "%02x".format(subDirId))
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

  def fileExists(blockId: BlockId): Boolean = {
    //zengdan
    // Figure out which tachyon directory it hashes to, and which subdirectory in that
    blockId.name.split("_")(0) match {

      case "operator" =>
        fileExists(blockId.name, tachyonGlobalDirs)
      case _ =>
        fileExists(blockId.name, tachyonDirs, subDirs) //original  zengdan
    }
  }

  //zengdan
  def fileExists(filename:String, dirs: Array[TachyonFile], subs: Array[Array[TachyonFile]]) = {
    //filename is identified by plan.id_partition.id
    //return subs(dirId)(subDirId)/filename
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % dirs.length
    val subDirId = (hash / dirs.length) % subDirsPerTachyonDir

    // Create the subdirectory if it doesn't already exist
    val subDir = subs(dirId)(subDirId)
    if (subDir == null) {
      false
    } else {
      val filePath = new TachyonURI(subDir + "/" + filename)
      client.exist(filePath) && client.getFile(filePath).isComplete
    }
  }

  //zengdan
  def fileExists(filename:String, dirs: Array[TachyonFile]) = {
    //filename is identified by plan.id_partition.id
    //return subs(dirId)(subDirId)/filename
    val hash = Utils.nonNegativeHash(filename)
    val dirId = hash % dirs.length

    val operatorId = filename.split("_")(1)
    if (dirs(dirId) == null) {
      false
    } else {
      val filePath = new TachyonURI(dirs(dirId).getPath + "/" + operatorId + "/" + filename)
      client.exist(filePath) && client.getFile(filePath).isComplete
    }
  }

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
object ReuseBlockManager extends Logging{
  def removeGlobalDir(conf: SparkConf) {
    logInfo("Remove global dir of tachyon storage")
    val master = conf.get("spark.tachyonStore.url",  "tachyon://localhost:19998")
    val client = if (master != null && master != "") TachyonFS.get(master) else null

    if (client == null) {
      logError("Failed to connect to the Tachyon as the master address is not configured")
      System.exit(ExecutorExitCode.EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE)
    }

    val root = new TachyonURI(conf.get("spark.tachyonStore.global.baseDir",
      "/global_spark_tachyon"))

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
    val conf = new SparkConf()
    val master = conf.get("spark.tachyonStore.url",  "tachyon://localhost:19998")
    val client = if (master != null && master != "") TachyonFS.get(master) else null

    if (client == null) {
      logError("Failed to connect to the Tachyon as the master address is not configured")
      System.exit(ExecutorExitCode.EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE)
    }

    val root = conf.get("spark.tachyonStore.global.baseDir", "/global_spark_tachyon")
    val path = new TachyonURI(root + "/" + id)
    client.exist(path)
  }
}


