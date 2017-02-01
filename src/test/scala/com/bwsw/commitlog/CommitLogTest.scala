package com.bwsw.commitlog

import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.io.{File, IOException, PrintWriter}
import java.util.Date

import com.bwsw.commitLog.CommitLog
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 27.01.17.
  */
class CommitLogTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val dir = "target/clt"
  val rec = "sample record".map(_.toByte).toArray

  override def beforeAll() = {
    new File(dir).mkdirs()
  }

  it should "write correctly" in {
    val cl = new CommitLog(1, dir)
    val f1 = cl.putRec(rec,0)
    Thread.sleep(1100)
    val f2 = cl.putRec(rec,0)
    cl.close()
    f1 == f2 shouldBe false
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
