package com.bwsw.commitlog.filesystem

import java.io.{File, FileInputStream, FileNotFoundException}
import java.math.BigInteger
import java.security.MessageDigest

import com.bwsw.commitlog.utils.Utils

import scala.io.Source

/** Represents commitlog file with data.
  *
  * @param path full path to file
  */
class CommitLogFile(path: String) {
  private val file = new File(path)
  private val md5File = new File(file.toString.split('.')(0) + ".md5")

  /** Returns underlying file. */
  def getFile(): File = {
    file
  }

  /** Returns an iterator over records */
  def getIterator(): CommitLogFileIterator = new CommitLogFileIterator(file.toString)

  /** Returns calculated MD5 of this file. */
  def calculateMD5(): String = {
    val fileInputStream = new FileInputStream(file)
    val stream = Utils.fileContentStream(fileInputStream, 512).takeWhile(elem => elem._1 != -1)
    val md5: MessageDigest = MessageDigest.getInstance("MD5")
    md5.reset()
    stream.foreach(elem => md5.update(elem._2.slice(0, elem._1)))
    fileInputStream.close()
    new BigInteger(1, md5.digest()).toString(16)
  }

  /** Returns a MD5 sum from MD5 FIle */
  private def getContentOfMD5File = Source.fromFile(md5File).getLines.mkString

  /** Returns existing MD5 of this file. Throws an exception otherwise. */
  def getMD5(): String = if (!md5File.exists()) throw new FileNotFoundException("No MD5 file for " + path) else getContentOfMD5File

  /** Checks md5 sum of file with existing md5 sum. Throws an exception when no MD5 exists. */
  def checkMD5(): Boolean = getMD5 == calculateMD5()

  /** Returns true if md5-file exists. */
  def md5Exists(): Boolean = md5File.exists()
}
