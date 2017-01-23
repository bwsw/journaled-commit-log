package bwsw

import java.io._
import java.math.BigInteger
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.Base64.{Decoder, Encoder}
import java.util.{Base64, Calendar}

import scala.util.matching.Regex

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

  def putRec(msg: Array[Byte], typ: Byte, startNew: Boolean): String = {
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

    val msgWithType: Array[Byte] = Array[Byte](typ) ++ msg
    Stream.continually(outputStream.write(Array[Byte](delimiter) ++ base64Encoder.encode(msgWithType)))
    outputStream.flush()

    md5.update(Array[Byte](delimiter))
    md5.update(base64Encoder.encode(msgWithType))

    return outputFileName
  }

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

  def perf(countOfRecords: Int, msg: Array[Byte], typ: Byte): Long = {
    require(countOfRecords > 0, "Count of records cannot be less than 1")

    val before = System.currentTimeMillis()
    for (i <- 1 to countOfRecords) putRec(msg, typ, startNew = false)
    System.currentTimeMillis() - before
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

  def endSession() = {
    if (!firstRun) {
      outputStream.close()
      writeMD5()
    }
  }

  private def getListOfFiles(path: String, extensions: List[String]): List[File] = {
    getListOfFiles(path).filter(_.isFile).filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  private def getListOfFiles(path: String): List[File] = {
    val file = new File(path)
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
