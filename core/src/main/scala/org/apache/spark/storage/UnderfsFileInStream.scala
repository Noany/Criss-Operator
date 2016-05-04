package org.apache.spark.storage

/**
 * Created by zengdan on 15-11-3.
 */


import java.io.IOException
import org.apache.spark.SparkEnv
import tachyon.client._
import tachyon.conf.TachyonConf

class UnderfsFileInStream(file: TachyonFile, opType: ReadType, ufsConf: Object, tachyonConf: TachyonConf){

}
/*
  extends FileInStream(file, opType, tachyonConf) {

  var mClosed = false;
  var mFileLength = file.length();
  var mBlockCapacity = file.getBlockSizeByte();
  var mCurrentPosition = 0L;
  var mCurrentBlockIndex = -1;
  var mCurrentBlockInStream: BlockInStream = null;
  var mCurrentBlockLeft = 0L;
  var mUFSConf = ufsConf;

  @throws(classOf[IOException])
  def this(file: TachyonFile, opType: ReadType, tachyonConf: TachyonConf) {
    this(file, opType, null, tachyonConf);
  }

  @throws(classOf[IOException])
  def checkAndAdvanceBlockInStream() {
    if(this.mCurrentBlockLeft == 0L) {
      if(this.mCurrentBlockInStream != null) {
        this.mCurrentBlockInStream.close();
      }

      this.mCurrentBlockIndex = getCurrentBlockIndex();

      this.mCurrentBlockInStream = new UnderfsBlockInStream(this.mFile, this.mReadType, this.mCurrentBlockIndex,
        this.mUFSConf, this.mTachyonConf);
      this.mCurrentBlockLeft = this.mBlockCapacity;
    }

  }


  def getCurrentBlockIndex():Int = {
    (this.mCurrentPosition / this.mBlockCapacity).asInstanceOf[Int];
  }

  @throws(classOf[IOException])
  override def read(): Int = {
    if(this.mCurrentPosition >= this.mFileLength) {
      return -1;
    } else {
      this.checkAndAdvanceBlockInStream();
      this.mCurrentPosition += 1;
      this.mCurrentBlockLeft -=1;
      return this.mCurrentBlockInStream.read();
    }
  }


  @throws(classOf[IOException])
  override def read(b: Array[Byte], off: Int, len: Int):Int = {
    if(b == null) {
      throw new NullPointerException();
    } else if(off >= 0 && len >= 0 && len <= b.length - off) {
      if(len == 0) {
        return 0;
      } else if(this.mCurrentPosition >= this.mFileLength) {
        return -1;
      } else {
        var tOff = off;
        var tLen = len;

        while(tLen > 0 && this.mCurrentPosition < this.mFileLength) {
          this.checkAndAdvanceBlockInStream();
          val tRead = this.mCurrentBlockInStream.read(b, tOff, tLen);
          if(tRead != -1) {
            this.mCurrentPosition += tRead;
            this.mCurrentBlockLeft -= tRead;
            tLen -= tRead;
            tOff += tRead;
          }
        }

        return len - tLen;
      }
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  @throws(classOf[IOException])
  override def seek(pos: Long) {
    if(this.mCurrentPosition != pos) {
      if(pos < 0L) {
        throw new IOException("Seek position is negative: " + pos);
      } else if(pos > this.mFileLength) {
        throw new IOException("Seek position is past EOF: " + pos + ", fileSize = " + this.mFileLength);
      } else {
        if((pos / this.mBlockCapacity).toInt != this.mCurrentBlockIndex) {
          this.mCurrentBlockIndex = (pos / this.mBlockCapacity).toInt;
          if(this.mCurrentBlockInStream != null) {
            this.mCurrentBlockInStream.close();
          }

          this.mCurrentBlockInStream = new UnderfsBlockInStream(this.mFile, this.mReadType, this.mCurrentBlockIndex, this.mUFSConf, this.mTachyonConf);
        }

        this.mCurrentBlockInStream.seek(pos % this.mBlockCapacity);
        this.mCurrentPosition = pos;
        this.mCurrentBlockLeft = this.mBlockCapacity - pos % this.mBlockCapacity;
      }
    }
  }

  @throws(classOf[IOException])
  override def skip(n: Long): Long = {
    if(n <= 0L) {
      return 0L;
    } else {
      var ret = n;
      if(this.mCurrentPosition + n >= this.mFile.length()) {
        ret = this.mFile.length() - this.mCurrentPosition;
        this.mCurrentPosition += ret;
      } else {
        this.mCurrentPosition += n;
      }

      val tBlockIndex = (this.mCurrentPosition / this.mBlockCapacity).toInt;
      var skip = 0L;
      if(tBlockIndex != this.mCurrentBlockIndex) {
        if(this.mCurrentBlockInStream != null) {
          this.mCurrentBlockInStream.close();
        }

        this.mCurrentBlockIndex = tBlockIndex;
        this.mCurrentBlockInStream = new UnderfsBlockInStream(this.mFile, this.mReadType, this.mCurrentBlockIndex, this.mUFSConf, this.mTachyonConf);
        skip = this.mCurrentPosition % this.mBlockCapacity;
        val skip1 = this.mCurrentBlockInStream.skip(skip);
        this.mCurrentBlockLeft = this.mBlockCapacity - skip1;
        if(skip1 != skip) {
          throw new IOException("The underlayer BlockInStream only skip " + skip1 + " instead of " + skip);
        }
      } else {
        skip = this.mCurrentBlockInStream.skip(ret);
        if(skip != ret) {
          throw new IOException("The underlayer BlockInStream only skip " + skip + " instead of " + ret);
        }
      }

      return ret;
    }
  }
}
*/

