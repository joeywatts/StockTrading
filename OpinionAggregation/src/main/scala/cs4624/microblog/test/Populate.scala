package cs4624.microblog.test

import cs4624.common.App
import cs4624.microblog.sources._
import cs4624.microblog.sources.HBaseMicroblogDataSource.Default

import java.time._

import org.apache.hadoop.hbase.client.ConnectionFactory

object Populate extends App {
  implicit val con = ConnectionFactory.createConnection()
  val filepath = args.headOption.getOrElse("../firms_11_2015.csv")
  println(s"reading posts from $filepath")
  val dataSource = new HBaseMicroblogDataSource(Default)
  val csv = new CsvMicroblogDataSource(filepath)
  val count = csv.query().foldLeft(0L) { (acc, post) =>
    if ((acc + 1) % 1024 == 0) println(s"written ${acc+1} posts so far....")
    dataSource.write(post)
    acc + 1
  }
  println(s"finished writing $count posts....")
}
