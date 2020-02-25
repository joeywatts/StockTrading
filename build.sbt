import sbtsparksubmit.SparkSubmitPlugin.autoImport._

lazy val sparkHbaseConnector = (project in file("./spark-hbase-connector"))
  .settings(Settings.commonSettings ++
    Dependencies.spark ++
    Seq(
      organization := "it.nerdammer.bigdata",
      name := "spark-hbase-connector",
      version := "1.0.3"
    )
  )

lazy val common = project
  .settings(
    Settings.commonSettings ++
      Dependencies.spark ++
      Dependencies.playWs ++
      Seq(name := "common")
  ).dependsOn(sparkHbaseConnector)

lazy val OpinionAggregation = project.dependsOn(common, PricingData)
  .settings(
    Settings.commonSettings ++ Dependencies.spark ++ Seq(
      name := "OpinionAggregation"
    ) ++ SparkSubmitSetting("runSpark", Seq("--class", "cs4624.microblog.Test"))
  )

lazy val PricingData = project.dependsOn(common)
  .settings(
    Settings.commonSettings ++ Dependencies.spark ++ Seq(
      name := "PricingData"
    )
  )

lazy val TradingSimulation = project.dependsOn(common, PricingData, OpinionAggregation)
  .settings(
    Settings.commonSettings ++ Dependencies.spark ++ Seq(
      name := "TradingSimulation"
    )
  )

lazy val root = (project in file("."))
  .aggregate(
    common,
    OpinionAggregation,
    PricingData,
    TradingSimulation
  )
  .settings(Sync.task)
retrieveManaged := true

// Exclude Emacs autosave files.
excludeFilter in unmanagedSources := ".#*"

