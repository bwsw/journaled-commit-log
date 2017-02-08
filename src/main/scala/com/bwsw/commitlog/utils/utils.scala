package com.bwsw.commitlog.utils

import java.io.FileInputStream

object utils {
  /** Converts FileInputStream to scala stream of pairs (count of bytes, Array[Byte]).
    *
    * @param fileIn file to convert to stream.
    * @param bufferSize size of buffer to use.
    * @return stream of pairs (count of bytes, Array[Byte]).
    */
  def fileContentStream(fileIn: FileInputStream, bufferSize: Int): Stream[(Int, Array[Byte])] = {
    val bytes = Array.fill[Byte](bufferSize)(0)
    val length = fileIn.read(bytes)
    (length, bytes) #:: fileContentStream(fileIn, bufferSize)
  }
}
