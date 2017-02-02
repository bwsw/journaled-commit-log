package com.bwsw.commitlog.filesystem

import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by zhdanovks on 31.01.17.
  */
class CommitLogFileTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val dir = "target/clf"

  override def beforeAll() = {
    new File(dir).mkdirs()
  }

  it should "compute and check md5 correctly" in {
    val pathEmptyFile: String = "target/clf/4444/44/44/0.dat"
    val pathEmptyFileMD5: String = "target/clf/4444/44/44/0.md5"
    val pathNotEmptyFile: String = "target/clf/4444/44/44/1.dat"
    val pathNotEmptyFileMD5: String = "target/clf/4444/44/44/1.md5"
    val pathNotEmptyFile2: String = "target/clf/4444/44/44/2.dat"
    val pathNotEmptyFile2MD5: String = "target/clf/4444/44/44/2.md5"
    val pathNotEmptyFile3: String = "target/clf/4444/44/44/3.dat"
    val md5EmptyFile: String = "d41d8cd98f00b204e9800998ecf8427e"
    val md5NotEmptyFile: String = "c4ca4238a0b923820dcc509a6f75849b"
    new File("target/clf/4444/44/44").mkdirs()
    new File(pathEmptyFile).createNewFile()
    new File(pathNotEmptyFile).createNewFile()
    new File(pathNotEmptyFile2).createNewFile()
    new File(pathNotEmptyFile3).createNewFile()
    new PrintWriter(pathEmptyFileMD5) {
      write(md5EmptyFile)
      close()
    }
    new PrintWriter(pathNotEmptyFile) {
      write("1")
      close()
    }
    new PrintWriter(pathNotEmptyFileMD5) {
      write(md5NotEmptyFile)
      close()
    }
    new PrintWriter(pathNotEmptyFile2) {
      write("2")
      close()
    }
    new PrintWriter(pathNotEmptyFile2MD5) {
      write(md5NotEmptyFile)
      close()
    }
    new PrintWriter(pathNotEmptyFile3) {
      write("3")
      close()
    }
    val clfEmpty = new CommitLogFile(pathEmptyFile)
    val clfNotEmpty = new CommitLogFile(pathNotEmptyFile)
    val clfNotEmpty2 = new CommitLogFile(pathNotEmptyFile2)
    val clfNotEmpty3 = new CommitLogFile(pathNotEmptyFile3)
    clfEmpty.calculateMD5() == md5EmptyFile shouldBe true
    clfNotEmpty.calculateMD5() == md5NotEmptyFile shouldBe true
    clfNotEmpty2.calculateMD5() == md5NotEmptyFile shouldBe false
    clfEmpty.checkMD5() shouldBe true
    clfNotEmpty.checkMD5() shouldBe true
    clfNotEmpty2.checkMD5() shouldBe false
    intercept[FileNotFoundException] {
      clfNotEmpty3.checkMD5()
    }
    intercept[FileNotFoundException] {
      clfNotEmpty3.getMD5()
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
