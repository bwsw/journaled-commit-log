package com.bwsw.commitlog

import java.io.{File, FilenameFilter}
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by Ivan Kudryavtsev on 26.01.2017.
  */

object FilePathManager {
  var CATALOGUE_GENERATOR = () => new SimpleDateFormat("yyyy/MM/dd").format(Calendar.getInstance.getTime)
}

class FilePathManager(rootDir: String) {
  private var curDate: String = FilePathManager.CATALOGUE_GENERATOR()
  private val rootPath = new File(rootDir)
  private var nextID = -1
  private val ext = ".dat"
  private val datFilter = new FilenameFilter() {
    override def accept(dir: File, name: String): Boolean = {
      name.toLowerCase().endsWith(ext)
    }
  }

  if(!rootPath.isDirectory())
    throw new IllegalArgumentException(s"Path $rootDir doesn't exists.")

  def getNextPath(): String = {

    val testDate: String = FilePathManager.CATALOGUE_GENERATOR()

    if(curDate != testDate) {
      nextID = -1
      curDate = testDate
    } else {
      if(nextID >= 0) {
        nextID += 1
        val nextPath = Paths.get(rootDir, testDate, nextID.toString).toString
        return nextPath
      }
    }

    if(createPath()) {
      nextID = 0
      return Paths.get(rootDir, testDate, nextID.toString).toString
    } else {
      val filesDir = new File(Paths.get(rootDir, testDate).toString)
      var max = -1

      filesDir.listFiles(datFilter).foreach(f => {
        if (Integer.parseInt(f.getName.split("\\.")(0)) > max) max = Integer.parseInt(f.getName.split("\\.")(0))
      })
      nextID = max + 1
      return Paths.get(rootDir, testDate, nextID.toString).toString
    }
  }

  private def createPath() = {
    val path = new File(Paths.get(rootDir, curDate).toString)
    if(!(path.exists() && path.isDirectory())) {
      path.mkdirs()
      true
    } else
      false
  }

}
