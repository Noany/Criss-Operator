package tachyon.client

import java.io.{InputStream, IOException}
import java.net.{InetSocketAddress, InetAddress}
import java.nio.ByteBuffer

import org.slf4j.{LoggerFactory, Logger}
import tachyon.client._
import _root_.tachyon.conf.TachyonConf
import tachyon.thrift.{NetAddress, ClientBlockInfo}
import tachyon.underfs.UnderFileSystem
import tachyon.util.NetworkUtils
import scala.collection.JavaConversions._

/**
 * Created by zengdan on 15-11-3.
 */
class UnderfsBlockInStream(file: TachyonFile, readType: ReadType, blockIndex: Int,
                            ufsConf: Object, tachyonConf: TachyonConf){

}
/*
  extends BlockInStream(file, readType, blockIndex, tachyonConf){

  require(mFile.isComplete, "File " + mFile.getPath() + " is not ready to read")

  val LOG: Logger = LoggerFactory.getLogger("");
  var mCheckpointInputStream: InputStream = null
  var mCheckpointPos = -1L
  var mBlockPos = 0L
  var mBlockOutStream: BlockOutStream = null
  var mUFSConf: Object = ufsConf;

  var mBlockInfo: ClientBlockInfo = mFile.getClientBlockInfo(this.mBlockIndex);
  var mRecache: Boolean = readType.isCache();


  @throws(classOf[IOException])
  def this(file: TachyonFile, readType: ReadType, blockIndex: Int, tachyonConf: TachyonConf) = {
    this(file, readType, blockIndex, null, tachyonConf)
  }


  @throws(classOf[IOException])
  private def cancelRecache() {
    if(this.mRecache) {
      this.mRecache = false;
      if(this.mBlockOutStream != null) {
        this.mBlockOutStream.cancel();
      }
    }

  }

  @throws(classOf[IOException])
  override def close(){
    if(!this.mClosed) {
      if(this.mRecache && this.mBlockOutStream != null) {
        if(this.mBlockPos == this.mBlockInfo.length) {
          this.mBlockOutStream.close();
        } else {
          this.mBlockOutStream.cancel();
        }
      }

      if(this.mCheckpointInputStream != null) {
        this.mCheckpointInputStream.close();
      }

      this.mClosed = true;
    }
  }

  @throws(classOf[IOException])
  override def read():Int = {
    val b = new Array[Byte](1);
    if(this.read(b) == -1){
      -1
    }else{
      b(0) & 255
    }
  }

  @throws(classOf[IOException])
  override def read(b: Array[Byte]) {
    return this.read(b, 0, b.length);
  }

  @throws(classOf[IOException])
  override def read(b: Array[Byte], offset: Int, len: Int) {
    var off = offset
    if(b == null) {
      throw new NullPointerException();
    } else if(off >= 0 && len >= 0 && len <= b.length - off) {
      if(len == 0) {
        return 0;
      } else if(this.mBlockPos == this.mBlockInfo.length) {
        return -1;
      } else {
        val newLen = Math.min(len.asInstanceOf[Long], this.mBlockInfo.length - this.mBlockPos).asInstanceOf[Int];
        var bytesLeft = newLen;
        if(newLen > 0 && this.mBlockOutStream == null && this.mRecache) {
          try {
            this.mBlockOutStream = BlockOutStream.get(this.mFile, WriteType.TRY_CACHE, this.mBlockIndex, this.mTachyonConf);
          } catch {
            case e: IOException =>
              LOG.warn("Recache attempt failed.", e);
              this.cancelRecache();
            case _ =>
          }
        }

        var readBytes = 0;

        if(bytesLeft > 0) {
          if(!this.setupStreamFromUnderFs()) {
            LOG.error("Failed to read at position " + this.mBlockPos + " in block " + this.mBlockInfo.getBlockId() + " from workers or underfs");
            return newLen - bytesLeft;
          }

          while(bytesLeft > 0) {
            readBytes = this.mCheckpointInputStream.read(b, off, bytesLeft);
            if(readBytes <= 0) {
              LOG.error("Checkpoint stream read 0 bytes, which shouldn\'t ever happen");
              return newLen - bytesLeft;
            }

            if(this.mRecache) {
              this.mBlockOutStream.write(b, off, readBytes);
            }

            off += readBytes;
            bytesLeft -= readBytes;
            this.mBlockPos += readBytes;
            this.mCheckpointPos += readBytes;
            this.mTachyonFS.getClientMetrics().incBytesReadUfs(readBytes);
          }
        }

        return newLen;
      }
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  @throws(classOf[IOException])
  override def seek(pos: Long) {
    if(pos < 0L) {
      throw new IOException("Seek position is negative: " + pos);
    } else if(pos > this.mBlockInfo.length) {
      throw new IOException("Seek position is past block size: " + pos + ", Block Size = " + this.mBlockInfo.length);
    } else if(pos != this.mBlockPos) {
      this.cancelRecache();
      this.mBlockPos = pos;
    }
  }

  @throws(classOf[IOException])
  private def setupStreamFromUnderFs(): Boolean = {
    if(this.mCheckpointInputStream == null || this.mBlockPos < this.mCheckpointPos) {
      val checkpointPath:String = this.mFile.getUfsPath();
      LOG.info("Opening stream from underlayer fs: " + checkpointPath);
      if(checkpointPath.equals("")) {
        return false;
      }

      val underfsClient = UnderFileSystem.get(checkpointPath, this.mUFSConf, this.mTachyonConf);
      this.mCheckpointInputStream = underfsClient.open(checkpointPath);
      if(this.mCheckpointInputStream.skip(this.mBlockInfo.offset) != this.mBlockInfo.offset) {
        throw new IOException("Failed to skip to the block offset " + this.mBlockInfo.offset + " in the checkpoint file");
      }

      this.mCheckpointPos = 0L;
    }

    while(this.mCheckpointPos < this.mBlockPos) {
      val skipped = this.mCheckpointInputStream.skip(this.mBlockPos - this.mCheckpointPos);
      if(skipped <= 0L) {
        throw new IOException("Failed to skip to the position " + this.mBlockPos + " for block " + this.mBlockInfo);
      }

      this.mCheckpointPos += skipped;
    }

    return true;
  }

  @throws(classOf[IOException])
  override def skip(n: Long): Long = {
    if(n <= 0L) {
      return 0L;
    } else {
      this.cancelRecache();
      val skipped = Math.min(n, this.mBlockInfo.length - this.mBlockPos);
      this.mBlockPos += skipped;
      return skipped;
    }
  }
}
*/
