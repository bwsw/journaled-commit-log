package com.bwsw.commitlog

/**
  * Created by Ivan Kudryavtsev on 26.01.17.
  */
object CommitLogFlushPolicy {
  /**
    * Basic trait for all policies
    */
  trait ICommitLogFlushPolicy

  case object OnRotation extends ICommitLogFlushPolicy

  case class OnTimeInterval(seconds: Integer) extends ICommitLogFlushPolicy {
    require(seconds > 0, "Interval of seconds must be greater that 0.")
  }

  case class OnCountInterval(count: Integer) extends ICommitLogFlushPolicy {
    require(count > 0, "Interval of writes must be greater that 0.")
  }
}
