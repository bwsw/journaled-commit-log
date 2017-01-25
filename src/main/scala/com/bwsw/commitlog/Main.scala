package com.bwsw.commitLog

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val storagePath = "/tmp/1"
    val commitLog: CommitLog = new CommitLog(2, storagePath)
    val messages = Array[String]("Hello", "world", "!", "Scala", "is", "great", "!")
    var filesPaths: Set[String] = Set[String]()

    println("Messages to encode:")
    for (message <- messages) {
      println(message)
    }

    for (message <- messages) {
      val outputFile: String = commitLog.putRec(message.map(_.toByte).to[Array], '.'.toByte, startNew = false)
      println(s"Output file: $outputFile")
      filesPaths = filesPaths + outputFile
      Thread.sleep(900)
    }

    commitLog.close()

    println("Decoded messages:")
    for (pathToFile <- filesPaths) {
      val msgs: IndexedSeq[Array[Byte]] = commitLog.getMessages(storagePath + "/" + pathToFile)
      for (msg <- msgs) {
        println(new String(msg))
      }
    }
  }
}
