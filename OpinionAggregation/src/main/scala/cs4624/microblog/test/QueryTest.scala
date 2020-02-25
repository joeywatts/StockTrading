package cs4624.microblog.test

import cs4624.common.App
import cs4624.microblog.sources.HBaseMicroblogDataSource
import cs4624.microblog.sources.HBaseMicroblogDataSource.Default

import java.time._

import org.apache.hadoop.hbase.client.ConnectionFactory

object QueryTest extends App {
  implicit val con = ConnectionFactory.createConnection()
  val dataSource = new HBaseMicroblogDataSource(Default)

  val start = LocalDate.of(2015, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC)
  val end = LocalDate.of(2015, 1, 3).atStartOfDay().toInstant(ZoneOffset.UTC)

  val posts = dataSource.query(startTime = Some(start), endTime = Some(end))
    .foreach(println)
}

