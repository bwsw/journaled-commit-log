package com.bwsw.commitLog

import java.io._
import java.math.BigInteger
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Base64.{Decoder, Encoder}
import java.util.{Base64, Calendar}

import scala.util.matching.Regex

/** Logger which stores records continuously in files in specified location.
  *
  * Stores data in files named YYYY.mm.dd.{serial number}. If it works correctly, md5-files named YYYY.mm.dd.{serial
  * number}.md5 shall be generated as well. New file starts on user request or when configured time was exceeded.
  *
  * @param seconds period of time to write records into the same file, then start new file.
  * @param path location to store files at.
  */
class CommitLog(seconds: Int, path: String) {
  require(seconds > 0, "Seconds cannot be less than 1")

  val nsecs: Int = seconds
  val storagePath: String = path
  private val regexFilterFiles: Regex =
    "^\\d{4}[\\/\\.](0?[1-9]|1[012])[\\/\\.](0?[1-9]|[12][0-9]|3[01])[\\/\\.]\\d+$".r
  private val base64Encoder: Encoder = Base64.getEncoder
  private val md5: MessageDigest = MessageDigest.getInstance("MD5")
  private val delimiter: Byte = 0

  private var listOfExistingFileNames: List[String] =
    getListOfFiles(storagePath).filter(x => regexFilterFiles.pattern.matcher(x.getName).matches).map(file => file
      .getName)
  private var outputFileName: String = getOutputFileName(listOfExistingFileNames)
  private var outputFilePath: String = storagePath + "/" + outputFileName
  private var lastTimeNewFileCreated: Long = -1
  private var outputStream: BufferedOutputStream = _

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
  def putRec(message: Array[Byte], messageType: Byte, startNew: Boolean): String = {
    if (startNew && !firstRun) {
      lastTimeNewFileCreated = -1
      outputStream.close()
      writeMD5()
    }

    if (firstRun || timeExceeded) {
      if (!firstRun) {
        outputStream.close()
        writeMD5()
      }
      outputFileName = getOutputFileName(listOfExistingFileNames)
      outputFilePath = storagePath + "/" + outputFileName
      listOfExistingFileNames = outputFileName :: listOfExistingFileNames
      lastTimeNewFileCreated = getCurrentSecs
      outputStream = new BufferedOutputStream(new FileOutputStream(outputFilePath, true))
    }

    val msgWithType: Array[Byte] = Array[Byte](messageType) ++ message
    Stream.continually(outputStream.write(Array[Byte](delimiter) ++ base64Encoder.encode(msgWithType)))
    outputStream.flush()

    md5.update(Array[Byte](delimiter))
    md5.update(base64Encoder.encode(msgWithType))

    return outputFileName
  }

  /** Return decoded messages from specified file.
    *
    * @param path path to file to read data from.
    * @return sequence of decoded messages.
    */
  def getMessages(path: String): IndexedSeq[Array[Byte]] = {
    val base64decoder: Decoder = Base64.getDecoder
    val byteArray = Files.readAllBytes(Paths.get(path))
    var msgs: IndexedSeq[Array[Byte]] = IndexedSeq[Array[Byte]]()
    var i = 0
    while (i < byteArray.length) {
      var msg: Array[Byte] = Array[Byte]()
      if (byteArray(i) == 0) {
        i += 1
        while (i < byteArray.length && byteArray(i) != 0.toByte) {
          msg = msg :+ byteArray(i)
          i += 1
        }
        msgs = msgs :+ base64decoder.decode(msg)
      } else {
        new Exception("No zero at the beginning of a message")
      }
    }
    return msgs
  }

  /** Performance test.
    *
    * Writes specified count of messages to file.
    *
    * @param countOfRecords count of records to write.
    * @param message message to write.
    * @param typeOfMessage type of message.
    * @return count of milliseconds writing to file took.
    */
  def perf(countOfRecords: Int, message: Array[Byte], typeOfMessage: Byte): Long = {
    require(countOfRecords > 0, "Count of records cannot be less than 1")

    val before = System.currentTimeMillis()
    for (i <- 1 to countOfRecords) putRec(message, typeOfMessage, startNew = false)
    System.currentTimeMillis() - before
  }

  /** Finishes work with current file.
    *
    */
  def endSession() = {
    if (!firstRun) {
      lastTimeNewFileCreated = -1
      outputStream.close()
      writeMD5()
    }
  }

  private def firstRun() = {
    lastTimeNewFileCreated == -1
  }

  private def writeMD5() = {
    val md5towrite: String = new BigInteger(1, md5.digest()).toString(16)
    new PrintWriter(outputFilePath + ".md5") {
      write(md5towrite)
      close()
    }
    md5.reset()
  }

  private def getCurrentSecs: Long = {
    System.currentTimeMillis() / 1000
  }

  private def timeExceeded(): Boolean = {
    getCurrentSecs - lastTimeNewFileCreated > nsecs
  }

  /** Return next name of file based on list of existing files.
    *
    * Return "YYY.mm.dd.1" if there are no files with names denote current day, otherwise return
    * "YYY.mm.dd.(next ordinal number)".
    */
  private def getOutputFileName(existingFiles: List[String]): String = {
    val curDate: String = new SimpleDateFormat("yyyy.MM.dd").format(Calendar.getInstance.getTime)
    if (existingFiles.nonEmpty) {
      val curDates: List[String] = existingFiles.filter(file => file.startsWith(curDate))
      if (curDates.nonEmpty) {
        val ordinalNumbers: List[Int] = curDates.map(x => x.split("\\.").last.toInt).sortWith(_ > _)
        return curDate + "." + (ordinalNumbers.head + 1)
      }
    }
    return curDate + ".1"
  }

  /** Returns list of files from specified directory.
    *
    */
  private def getListOfFiles(pathToFolder: String): List[File] = {
    val file = new File(pathToFolder)
    if (file.exists) {
      if (file.isDirectory) {
        file.listFiles.filter(_.isFile).toList
      }
      else {
        throw new IllegalArgumentException("Not a directory: " + file)
      }
    } else {
      throw new IllegalArgumentException("Path does not exists: " + file)
    }
  }
}
