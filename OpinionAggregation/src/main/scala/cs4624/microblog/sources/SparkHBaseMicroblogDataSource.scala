package cs4624.microblog.sources

import java.time.Instant

import cs4624.microblog.sentiment.{Bearish, Bullish}
import cs4624.microblog.{MicroblogAuthor, MicroblogPost}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import it.nerdammer.spark.hbase._

/**
  * Created by joeywatts on 2/28/17.
  */
class SparkHBaseMicroblogDataSource(table: SparkHBaseMicroblogDataSource.Table)
                                   (implicit val sc: SparkContext)
  extends MicroblogDataSource {

  override def query(startTime: Option[Instant],
                     endTime: Option[Instant]): Iterator[MicroblogPost] = {
    queryRDD(startTime, endTime).toLocalIterator
  }

  def queryRDD(startTime: Option[Instant] = None,
               endTime: Option[Instant] = None): RDD[MicroblogPost] = {
    val select = sc.hbaseTable[(String, String, String, String, Option[String], Option[String])](table.name)
      .select("base_data:timestamp", "base_data:text", "base_data:judgeid",
        "options:sentiment", "options:symbol")
    val startRowSet = startTime match {
      case Some(time) =>
        select.withStartRow(time.toEpochMilli + "-")
      case None => select
    }
    val endRowSet = endTime match {
      case Some(time) =>
        startRowSet.withStopRow((time.toEpochMilli+1) + "-")
      case None => startRowSet
    }
    endRowSet.map { case (row, timestamp, text, judgeid, sentiment, symbol) =>
      MicroblogPost(
        id = row.substring(row.indexOf("-")+1),
        text = text,
        author = MicroblogAuthor(judgeid),
        sentiment = sentiment match {
          case Some("Bullish") => Some(Bullish)
          case Some("Bearish") => Some(Bearish)
          case _ => None
        },
        time = Instant.parse(timestamp),
        symbols = symbol.getOrElse("").split("::::").toSet
      )
    }
  }

  def write(posts: RDD[MicroblogPost]) = {
    posts.map(post => {
      (post.time.toEpochMilli + "-" + post.id,
        post.time.toString,
        post.text,
        post.author.id,
        post.sentiment.map(_.toString),
        if (post.symbols.isEmpty) None else Some(post.symbols.mkString("::::")))
    }).toHBaseTable(table.name)
      .toColumns("base_data:timestamp", "base_data:text", "base_data:judgeid",
        "options:sentiment", "options:symbol")
      .save()
  }

}

object SparkHBaseMicroblogDataSource {
  sealed trait Table { def name: String }
  case object Default extends Table {
    override def name = "stocktwits_timestamped"
  }
}
