package cs4624.microblog.sources

import java.time.Instant

import cs4624.microblog.sentiment.{Bearish, Bullish}
import cs4624.microblog.{MicroblogAuthor, MicroblogPost}
import cs4624.microblog.sources.HBaseMicroblogDataSource.Default
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

/**
  * Created by joeywatts on 3/20/17.
  */
class HBaseMicroblogDataSource(table: HBaseMicroblogDataSource.Table = Default)
                              (implicit hbaseConnection: Connection)
  extends MicroblogDataSource {

  private val hbaseTable = hbaseConnection.getTable(TableName.valueOf(table.name))

  private val baseDataCF = Bytes.toBytes("base_data")
  private val optionsCF = Bytes.toBytes("options")

  private val timestampCQ = Bytes.toBytes("timestamp")
  private val textCQ = Bytes.toBytes("text")
  private val judgeCQ = Bytes.toBytes("judgeid")
  private val sentimentCQ = Bytes.toBytes("sentiment")
  private val symbolCQ = Bytes.toBytes("symbol")

  override def query(startTime: Option[Instant],
                     endTime: Option[Instant]): Iterator[MicroblogPost] = {
    val scan = new Scan()
    startTime match {
      case Some(time) =>
        scan.setStartRow(Bytes.toBytes(time.toEpochMilli + "-"))
      case _ =>
    }
    endTime match {
      case Some(time) =>
        scan.setStopRow(Bytes.toBytes((time.toEpochMilli + 1) + "-"))
      case _ =>
    }
    hbaseTable.getScanner(scan).iterator().map(resultToMicroblogPost)
  }

  private def resultToMicroblogPost(result: Result): MicroblogPost = {
    val row = Bytes.toString(result.getRow)
    val id = row.substring(row.indexOf("-") + 1)
    val timestamp = Bytes.toString(result.getValue(baseDataCF, timestampCQ))
    val text = Bytes.toString(result.getValue(baseDataCF, textCQ))
    val judge = Bytes.toString(result.getValue(baseDataCF, judgeCQ))
    val sentiment = Option(result.getValue(optionsCF, sentimentCQ)).map(Bytes.toString) match {
      case Some("Bullish") => Some(Bullish)
      case Some("Bearish") => Some(Bearish)
      case _ => None
    }
    val symbol = Option(result.getValue(optionsCF, symbolCQ)).map(Bytes.toString) match {
      case Some(str) => str.split("::::").toSet
      case _ => Set[String]()
    }
    MicroblogPost(
      id = id,
      text = text,
      author = MicroblogAuthor(judge),
      time = Instant.parse(timestamp),
      sentiment = sentiment,
      symbols = symbol
    )
  }

  def write(post: MicroblogPost): Unit = {
    val put = new Put(Bytes.toBytes(post.time.toEpochMilli + "-" + post.id))
    put.addColumn(baseDataCF, textCQ, Bytes.toBytes(post.text))
    put.addColumn(baseDataCF, timestampCQ, Bytes.toBytes(post.time.toString))
    put.addColumn(baseDataCF, judgeCQ, Bytes.toBytes(post.author.id))
    post.sentiment match {
      case Some(Bullish) =>
        put.addColumn(optionsCF, sentimentCQ, Bytes.toBytes("Bullish"))
      case Some(Bearish) =>
        put.addColumn(optionsCF, sentimentCQ, Bytes.toBytes("Bearish"))
      case _ =>
    }
    if (post.symbols.nonEmpty) {
      put.addColumn(optionsCF, symbolCQ, Bytes.toBytes(post.symbols.mkString("::::")))
    }
    hbaseTable.put(put)
  }
}
object HBaseMicroblogDataSource {
  sealed trait Table { def name: String }
  case object Default extends Table {
    override def name = "stocktwits_timestamped"
  }
}
