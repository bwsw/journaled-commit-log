name := "journaled-commit-log"

val jclVersion = "1.0.1-RC1"

version := jclVersion


scalaVersion := "2.12.1"

scalacOptions += "-feature"
scalacOptions += "-deprecation"

publishMavenStyle := true

organization := "com.bwsw"
licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
homepage := Some(url("https://github.com/bwsw/journaled-commit-log"))

pomIncludeRepository := { _ => false }

pomExtra := (
  <scm>
    <url>git@github.com:bwsw/journaled-commit-log.git</url>
    <connection>scm:git@github.com:bwsw/journaled-commit-log.git</connection>
  </scm>
    <developers>
      <developer>
        <id>bitworks</id>
        <name>Bitworks Software, Ltd.</name>
        <url>http://bitworks.software/</url>
      </developer>
    </developers>)

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false
assemblyJarName in assembly := s"${name.value}-${jclVersion}.jar"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.scalatest" % "scalatest_2.12" % "3.0.1")

assemblyMergeStrategy in assembly := {
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}