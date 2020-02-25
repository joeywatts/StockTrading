import sbt._
import Keys._

object Settings {

  val commonSettings = Seq(
    organization := "cs4624",
    scalaVersion := "2.10.5"
  ) ++ Dependencies.scalaTest

}
