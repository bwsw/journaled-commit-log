package com.bwsw.commitlog.filesystem

import java.io.{File, FileInputStream, IOException}
import java.util.Base64

import com.bwsw.commitlog.utils.utils

/** Iterator over records of the commitlog file.
  *
  * @param path full path to file
  */
class CommitLogFileIterator(path: String) extends Iterator[Array[Byte]] {
  private var isClosed: Boolean = false
  private val fileInputStream = new FileInputStream(new File(path)) {
    @Override
    override def close(): Unit = {
      super.close()
      isClosed = true
    }
  }
  private var stream = utils.fileContentStream(fileInputStream)
  private var nextRecord: Option[Array[Byte]] = getNextRecord()

  override def hasNext(): Boolean = nextRecord.isDefined

  override def next(): Array[Byte] = {
    val res = nextRecord.get
    fetchNextRecord()
    res
  }

  def close() = {
    if (!isClosed)
      fileInputStream.close()
  }

  private def fetchNextRecord() = {
    nextRecord = getNextRecord()
  }

  /** Returns next record from file whether it exists. */
  private def getNextRecord(): Option[Array[Byte]] = {
    var res: Array[Byte] = new Array[Byte](0)
    if (stream.head._1) {
      if (stream.head._2.get == 0) {
        stream = stream.tail
      } else {
        throw new IOException("Missing separator at the beginning of file " + path)
      }
      while (stream.head._1 && stream.head._2.get != 0) {
        res = res :+ stream.head._2.get
        stream = stream.tail
      }
    } else {
      close()
      return None
    }
    return Some(Base64.getDecoder.decode(res))
  }
}
