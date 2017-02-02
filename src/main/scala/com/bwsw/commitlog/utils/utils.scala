package com.bwsw.commitlog.utils

import java.io.FileInputStream

object utils {
  /** Converts FileInputStream to scala stream of pairs (still have data, Option[byte]). */
  def fileContentStream(fileIn: FileInputStream): Stream[(Boolean, Option[Byte])] = {
    val bytes = Array.fill[Byte](1)(0)
    val length = fileIn.read(bytes)
    (length > 0, if (length > 0) Some(bytes.head) else None) #:: fileContentStream(fileIn)
  }
}
