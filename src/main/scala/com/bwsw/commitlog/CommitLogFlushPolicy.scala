package com.bwsw.commitlog

/** Policies to work with commitlog. */
object CommitLogFlushPolicy {
  /** Basic trait for all policies */
  trait ICommitLogFlushPolicy

  /** User must decide by himself when start to write records to new file. */
  case object OnRotation extends ICommitLogFlushPolicy

  /** New file starts when specified count of seconds from last writing operation pass. */
  case class OnTimeInterval(seconds: Integer) extends ICommitLogFlushPolicy {
    require(seconds > 0, "Interval of seconds must be greater that 0.")
  }

  /** New file starts after specified count of write operations. */
  case class OnCountInterval(count: Integer) extends ICommitLogFlushPolicy {
    require(count > 0, "Interval of writes must be greater that 0.")
  }
}
