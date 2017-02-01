package com.bwsw.commitlog

import java.io.{File, FileInputStream, IOException}
import java.util.Base64
import java.util.Base64.Decoder

/**
  * Created by zhdanovks on 31.01.17.
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
  private var stream: Stream[(Int, Byte)] = fileContentStream(fileInputStream)
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

  private def getNextRecord(): Option[Array[Byte]] = {
    var res: Array[Byte] = new Array[Byte](0)
    if (stream.head._1 > 0) {
      if (stream.head._2 == 0) {
        stream = stream.tail
      } else {
        throw new IOException("Missing separator in file " + path)
      }
      while (stream.head._1 > 0 && stream.head._2 != 0) {
        res = res :+ stream.head._2
        stream = stream.tail
      }
    } else {
      close()
      return None
    }
    return Some(Base64.getDecoder.decode(res))
  }

  private def fetchNextRecord() = {
    nextRecord = getNextRecord()
  }

  private def fileContentStream(fileIn: FileInputStream): Stream[(Int, Byte)] = {
    val bytes = Array.fill[Byte](1)(0)
    val length = fileIn.read(bytes)
    (length, bytes(0)) #:: fileContentStream(fileIn)
  }
}
