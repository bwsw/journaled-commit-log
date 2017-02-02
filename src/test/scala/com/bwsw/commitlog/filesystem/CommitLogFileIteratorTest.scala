package com.bwsw.commitlog.filesystem

import java.io.{BufferedOutputStream, File, FileOutputStream, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.bwsw.commitLog.CommitLog
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by zhdanovks on 31.01.17.
  */
class CommitLogFileIteratorTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val dir = "target/clfi"

  override def beforeAll() = {
    new File(dir).mkdirs()
  }

  it should "read record from file" in {
    val commitLog = new CommitLog(1, dir)
    val fileName = commitLog.putRec(Array[Byte](2, 3, 4), 1, startNew = false)
    val commitLogFileIterator = new CommitLogFileIterator(fileName)
    if (commitLogFileIterator.hasNext) {
      commitLogFileIterator.next.deep == Array[Byte](1, 2, 3, 4).deep shouldBe true
    }
    commitLogFileIterator.hasNext shouldBe false
  }

  it should "read several records from file correctly" in {
    val commitLog = new CommitLog(10, dir)
    commitLog.putRec(Array[Byte](6, 7, 8), 5, startNew = false)
    commitLog.putRec(Array[Byte](7, 8, 9), 6, startNew = false)
    val fileName = commitLog.putRec(Array[Byte](2, 3, 4), 1, startNew = false)
    commitLog.close()
    val commitLogFileIterator = new CommitLogFileIterator(fileName)
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext) {
      commitLogFileIterator.next.deep == Array[Byte](5, 6, 7, 8).deep shouldBe true
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext) {
      commitLogFileIterator.next.deep == Array[Byte](6, 7, 8, 9).deep shouldBe true
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext) {
      commitLogFileIterator.next.deep == Array[Byte](1, 2, 3, 4).deep shouldBe true
    }
    commitLogFileIterator.hasNext shouldBe false
    commitLogFileIterator.close()
  }

  it should "read as much records from corrupted file as it can" in {
    val commitLog = new CommitLog(10, dir)
    commitLog.putRec(Array[Byte](6, 7, 8), 5, startNew = false)
    commitLog.putRec(Array[Byte](7, 8, 9), 6, startNew = false)
    val fileName = commitLog.putRec(Array[Byte](2, 3, 4), 1, startNew = false)
    commitLog.close()

    val bytesArray: Array[Byte] = Files.readAllBytes(Paths.get(fileName))

    val croppedFileName = fileName + ".cropped"
    val outputStream = new BufferedOutputStream(new FileOutputStream(croppedFileName))
    Stream.continually(outputStream.write(bytesArray.slice(0, 22)))
    outputStream.close()

    val commitLogFileIterator = new CommitLogFileIterator(croppedFileName)
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext) {
      commitLogFileIterator.next.deep == Array[Byte](5, 6, 7, 8).deep shouldBe true
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext) {
      commitLogFileIterator.next.deep == Array[Byte](6, 7, 8, 9).deep shouldBe true
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext) {
      commitLogFileIterator.next.deep == Array[Byte](1, 2, 3, 4).deep shouldBe false
    }
    commitLogFileIterator.hasNext shouldBe false
    commitLogFileIterator.close()
  }

  it should "Throw IOException when separator at the beginning of file is missing" in {
    val commitLog = new CommitLog(10, dir)
    commitLog.putRec(Array[Byte](6, 7, 8), 5, startNew = false)
    commitLog.putRec(Array[Byte](7, 8, 9), 6, startNew = false)
    commitLog.putRec(Array[Byte](5, 7, 9), 3, startNew = false)
    val fileName = commitLog.putRec(Array[Byte](7, 8, 9), 6, startNew = false)
    commitLog.close()

    val bytesArray: Array[Byte] = Files.readAllBytes(Paths.get(fileName))

    val croppedFileName = fileName + ".cropped"
    val outputStream = new BufferedOutputStream(new FileOutputStream(croppedFileName))
    Stream.continually(outputStream.write(bytesArray.slice(1, 35)))
    outputStream.close()

    intercept[IOException] {
      val commitLogFileIterator = new CommitLogFileIterator(croppedFileName)
    }
  }

  override def afterAll = {
    List(dir).foreach(dir =>
      Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }))
  }
}
