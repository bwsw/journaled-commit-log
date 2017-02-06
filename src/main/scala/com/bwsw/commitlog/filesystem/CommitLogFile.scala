package com.bwsw.commitlog.filesystem

import java.io.{File, FileInputStream, FileNotFoundException}
import java.math.BigInteger
import java.security.MessageDigest

import com.bwsw.commitlog.utils.utils

import scala.io.Source

/** Represents commitlog file with data.
  *
  * @param path full path to file
  */
class CommitLogFile(path: String) {
  private val file = new File(path)
  private val md5File = new File(file.toString.split('.')(0) + ".md5")
  private var md5: Option[String] = None
  if (md5File.exists()) {
    md5 = Some(Source.fromFile(md5File).getLines.mkString)
  }

  /** Returns underlying file. */
  def getFile(): File = {
    file
  }

  /** Returns an iterator over records */
  def getIterator(): CommitLogFileIterator = new CommitLogFileIterator(file.toString)

  /** Returns calculated MD5 of this file. */
  def calculateMD5(): String = {
    val fileInputStream = new FileInputStream(file)
    val stream = utils.fileContentStream(fileInputStream) takeWhile { chunk => chunk._1 }
    val md5: MessageDigest = MessageDigest.getInstance("MD5")
    md5.reset()
    stream.foreach(elem => if (elem._1) md5.update(elem._2.get))
    fileInputStream.close()
    new BigInteger(1, md5.digest()).toString(16)
  }

  /** Returns existing MD5 of this file. Throws an exception otherwise. */
  def getMD5(): String = {
    if (md5.isEmpty) {
      throw new FileNotFoundException("No MD5 file for " + path)
    } else {
      md5.get
    }
  }

  /** Checks md5 sum of file with existing md5 sum. Throws an exception when no MD5 exists. */
  def checkMD5(): Boolean = {
    getMD5() == calculateMD5()
  }

  /** Returns true if md5-file exists. */
  def md5Exists(): Boolean = {
    !md5.isEmpty
  }
}
