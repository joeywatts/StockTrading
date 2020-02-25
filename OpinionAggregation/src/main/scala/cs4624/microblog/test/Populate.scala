package cs4624.microblog.test

import cs4624.common.App
import cs4624.common.spark.SparkContextManager._
import cs4624.common.hbase.HBaseConnectionManager._
import cs4624.microblog.sources._
import cs4624.microblog.sources.SparkHBaseMicroblogDataSource.Default

import java.time._

object Populate extends App {
  val filepath = args.headOption.getOrElse("../firms_11_2015.csv")
  println(s"reading posts from $filepath")
  val dataSource = new SparkHBaseMicroblogDataSource(Default)
  val csv = CsvMicroblogDataSource(filepath)
  dataSource.write(csv.asRDD)
}
