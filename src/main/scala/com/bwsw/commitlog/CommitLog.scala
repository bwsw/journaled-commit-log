package com.bwsw.commitLog

import java.io._
import java.math.BigInteger
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Base64.{Decoder, Encoder}
import java.util.{Base64, Calendar, Date}

import com.bwsw.commitlog.{CommitLogFile, CommitLogFlushPolicy, FilePathManager}
import com.bwsw.commitlog.CommitLogFlushPolicy.{ICommitLogFlushPolicy, OnCountInterval, OnTimeInterval}


object CommitLog {
  val MD5EXTENSION = ".md5"
}

/** Logger which stores records continuously in files in specified location.
  *
  * Stores data in files named YYYY.mm.dd.{serial number}. If it works correctly, md5-files named YYYY.mm.dd.{serial
  * number}.md5 shall be generated as well. New file starts on user request or when configured time was exceeded.
  *
  * @param seconds period of time to write records into the same file, then start new file.
  * @param path location to store files at.
  */
class CommitLog(seconds: Int, path: String, policy: ICommitLogFlushPolicy = CommitLogFlushPolicy.OnRotation) {
  require(seconds > 0, "Seconds cannot be less than 1")

  private val secondsInterval: Int = seconds
  private val filePathManager: FilePathManager = new FilePathManager(path)

  private val base64Encoder: Encoder = Base64.getEncoder
  private val md5: MessageDigest = MessageDigest.getInstance("MD5")
  private val delimiter: Byte = 0

  private var fileCreationTime: Long = -1
  private var outputStream: BufferedOutputStream = _

  private var chunkWriteCount: Int = 0
  private var chunkOpenTime: Long = 0

  /** Puts record and its type to an appropriate file.
    *
    * Writes data to file in format (delimiter)(BASE64-encoded type and message). When writing to one file finished,
    * md5-sum file generated.
    *
    * @param message message to store.
    * @param messageType type of message to store.
    * @param startNew start new file if true.
    * @return name of file record was saved in.
    */
  def putRec(message: Array[Byte], messageType: Byte, startNew: Boolean = false): String = {

    if (startNew && !firstRun) {
      resetCounters()
      outputStream.close()
      writeMD5()
      filePathManager.getNextPath()
    }

    if (firstRun() || timeExceeded()) {
      if (!firstRun()) {
        resetCounters()
        outputStream.close()
        writeMD5()
      }
      filePathManager.getNextPath()
      fileCreationTime = getCurrentSecs()
      outputStream = new BufferedOutputStream(new FileOutputStream(filePathManager.getCurrentPath() + FilePathManager.EXTENSION, true))
    }

    chunkWriteCount += 1

    val encodedMsgWithType: Array[Byte] = base64Encoder.encode(Array[Byte](messageType) ++ message)
    Stream.continually(outputStream.write(Array[Byte](delimiter) ++ encodedMsgWithType))

    val now: Long = System.currentTimeMillis()
    if(policy.isInstanceOf[OnTimeInterval] && policy.asInstanceOf[OnTimeInterval].seconds * 1000 + chunkOpenTime < now) {
      chunkOpenTime = now
      flushStream()
    }
    else {
      if (policy.isInstanceOf[OnCountInterval] && policy.asInstanceOf[OnCountInterval].count == chunkWriteCount) {
        chunkWriteCount = 0
        flushStream()
      }
    }

    md5.update(Array[Byte](delimiter))
    md5.update(encodedMsgWithType)

    return filePathManager.getCurrentPath() + FilePathManager.EXTENSION
  }

  /** Finishes work with current file.
    *
    */
  def close() = {
    if (!firstRun) {
      resetCounters()
      outputStream.close()
      writeMD5()
    }
  }

//  /** Return decoded messages from specified file.
//    *
//    * @param path path to file to read data from.
//    * @return sequence of decoded messages.
//    */
//  def getMessages(path: String): IndexedSeq[Array[Byte]] = {
//    val base64decoder: Decoder = Base64.getDecoder
//    val byteArray = Files.readAllBytes(Paths.get(path))
//    var msgs: IndexedSeq[Array[Byte]] = IndexedSeq[Array[Byte]]()
//    var i = 0
//    while (i < byteArray.length) {
//      var msg: Array[Byte] = Array[Byte]()
//      if (byteArray(i) == 0) {
//        i += 1
//        while (i < byteArray.length && byteArray(i) != 0.toByte) {
//          msg = msg :+ byteArray(i)
//          i += 1
//        }
//        msgs = msgs :+ base64decoder.decode(msg)
//      } else {
//        new Exception("No zero at the beginning of a message")
//      }
//    }
//    return msgs
//  }

//  /** Performance test.
//    *
//    * Writes specified count of messages to file.
//    *
//    * @param countOfRecords count of records to write.
//    * @param message message to write.
//    * @param typeOfMessage type of message.
//    * @return count of milliseconds writing to file took.
//    */
//  def perf(countOfRecords: Int, message: Array[Byte], typeOfMessage: Byte): Long = {
//    require(countOfRecords > 0, "Count of records cannot be less than 1")
//
//    val before = System.currentTimeMillis()
//    for (i <- 1 to countOfRecords) putRec(message, typeOfMessage, startNew = false)
//    System.currentTimeMillis() - before
//  }

  private def flushStream() = {
    outputStream.flush()
  }

  private def resetCounters() = {
    fileCreationTime = -1
    chunkWriteCount = 0
    chunkOpenTime = System.currentTimeMillis()
  }

  private def firstRun() = {
    fileCreationTime == -1
  }

  private def writeMD5() = {
    val fileMD5: String = new BigInteger(1, md5.digest()).toString(16)
    new PrintWriter(filePathManager.getCurrentPath() + CommitLog.MD5EXTENSION) {
      write(fileMD5)
      close()
    }
    md5.reset()
  }

  private def getCurrentSecs(): Long = {
    System.currentTimeMillis() / 1000
  }

  private def timeExceeded(): Boolean = {
    getCurrentSecs - fileCreationTime >= secondsInterval
  }

}
