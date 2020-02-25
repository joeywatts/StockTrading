import sbt._
import Keys._

object Dependencies {

  val sparkVersion = "1.6.3"
  val hbaseVersion = "1.2.4"

  // Spark with HBase
  val spark = Seq(
    resolvers ++= Seq(
      "Cloudera repos" at "https://repository.cloudera.com/artifactory/cloudera-repos",
      "Cloudera releases" at "https://repository.cloudera.com/artifactory/libs-release"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.hbase" % "hbase-common" % hbaseVersion,
      "org.apache.hbase" % "hbase-client" % hbaseVersion,
      "org.apache.hbase" % "hbase-server" % hbaseVersion
    )
  )

  // ScalaTest libraries.
  val scalaTest = Seq(
    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )

  // Play WS library.
  val playWs = Seq(
    libraryDependencies += "com.typesafe.play" %% "play-ws" % "2.4.10"
  )
}
