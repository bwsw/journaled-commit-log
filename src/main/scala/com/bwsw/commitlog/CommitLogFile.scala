package com.bwsw.commitlog

import java.io.{File, FileInputStream, FileNotFoundException}
import java.math.BigInteger
import java.security.MessageDigest

import scala.io.Source

/**
  * Created by zhdanovks on 31.01.17.
  */
class CommitLogFile(path: String) {
  val file = new File(path)
  val md5File = new File(file.toString.split('.')(0) + ".md5")
  var md5: Option[String] = None
  if (md5File.exists()) {
    md5 = Some(Source.fromFile(md5File).getLines.mkString)
  }

  def getIterator(): CommitLogFileIterator = new CommitLogFileIterator(file.toString)

  /** Returns calculated MD5 of this file. */
  def calculateMD5(): String = {
    val fileInputStream = new FileInputStream(file)
    val stream = fileContentStream(fileInputStream) takeWhile { chunk => chunk._1 > 0 }
    val md5: MessageDigest = MessageDigest.getInstance("MD5")
    md5.reset()
    stream.foreach(elem => md5.update(elem._2, 0, elem._1))
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

  /** Checks md5 sum of file with existing md5 sum. */
  def checkMD5(): Boolean = {
    getMD5() == calculateMD5()
  }

  private def fileContentStream(fileIn: FileInputStream): Stream[(Int, Array[Byte])] = {
    val bytes = Array.fill[Byte](1024)(0)
    val length = fileIn.read(bytes)
    (length, bytes) #:: fileContentStream(fileIn)
  }
}
