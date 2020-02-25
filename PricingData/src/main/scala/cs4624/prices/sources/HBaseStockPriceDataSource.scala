package cs4624.prices.sources

import java.time.Instant

import cs4624.prices.StockPrice
import it.nerdammer.spark.hbase._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * Created by joeywatts on 2/27/17.
  */
class HBaseStockPriceDataSource(val table: HBaseStockPriceDataSource.Table)
                               (implicit val connection: Connection, implicit val sc: SparkContext)
  extends StockPriceDataSource {

  private val hbaseTable = connection.getTable(TableName.valueOf(table.name))

  override def priceAtTime(symbol: String, time: Instant): Option[StockPrice] = {
    // this uses the HBase java API instead of the Spark API because
    // the Spark API doesn't support reverse scans.
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(s"${symbol}_${Long.MaxValue - time.toEpochMilli}"))
    val scanner = hbaseTable.getScanner(scan)
    val result = scanner.next()
    if (result == null) {
      //println(s"Null result for ($symbol, $time)")
      None
    } else {
      val stockPrice = resultToStockPrice(result)
      if (stockPrice.symbol == symbol)
        Some(stockPrice)
      else
        None
    }

    /*sc.hbaseTable[(String, String)](table.name)
      .select("price:price")
      .withStartRow(s"${symbol}_${Long.MaxValue - time.toEpochMilli}")
      .take(1)
      .map { case (row, price) =>
        val s = row.split("_")
        StockPrice(s(0), Instant.ofEpochMilli(Long.MaxValue - s(1).toLong), price.toDouble)
      }.headOption*/
  }

  def resultToStockPrice(result: Result): StockPrice = {
    val rowKey = Bytes.toString(result.getRow)
    val splitRowKey = rowKey.split("_")
    val symbol = splitRowKey(0)
    val time = Instant.ofEpochMilli(Long.MaxValue - splitRowKey(1).toLong)
    val price = Bytes.toString(result.getValue(HBaseStockPriceDataSource.priceCF, HBaseStockPriceDataSource.priceCQ))
    StockPrice(symbol, time, BigDecimal(price))
  }

  override def query(symbol: String,
                     startTime: Option[Instant],
                     endTime: Option[Instant]): Iterator[StockPrice] = {
    val scan = new Scan()
    val startTimeMillis = startTime.map(_.toEpochMilli).getOrElse(0L)
    val endTimeMillis = endTime.map(_.toEpochMilli).getOrElse(Long.MaxValue)
    val startRow = s"${symbol}_${Long.MaxValue - endTimeMillis}"
    val endRow = s"${symbol}_${Long.MaxValue - startTimeMillis}"
    println(s"startRow = $startRow, endRow = $endRow")
    scan.setStartRow(Bytes.toBytes(startRow))
    scan.setStopRow(Bytes.toBytes(endRow))
    val scanner = hbaseTable.getScanner(scan)
    scanner.iterator().map(resultToStockPrice)
  }

  def queryRDD(symbol: String,
    startTime: Option[Instant],
    endTime: Option[Instant]): RDD[StockPrice] = {

    val startTimeMillis = startTime.getOrElse(Instant.MIN).toEpochMilli
    val endTimeMillis = endTime.getOrElse(Instant.MAX).toEpochMilli
    sc.hbaseTable[(String, String)](table.name)
      .select("price:price")
      .withStartRow(s"${symbol}_${Long.MaxValue - startTimeMillis}")
      .withStopRow(s"${symbol}_${Long.MaxValue - endTimeMillis}")
      .map { case (row, price) =>
        val s = row.split("_")
        StockPrice(s(0), Instant.ofEpochMilli(s(1).toLong), BigDecimal(price))
      }
  }

  def rowKey(symbol: String, time: Instant): Array[Byte] = Bytes.toBytes(symbol + "_" + (Long.MaxValue - time.toEpochMilli))

  def write(price: StockPrice): Unit = {
    val put = new Put(Bytes.toBytes(s"${price.symbol}_${Long.MaxValue - price.time.toEpochMilli}"))
    put.addColumn(
      HBaseStockPriceDataSource.priceCF,
      HBaseStockPriceDataSource.priceCQ,
      Bytes.toBytes(price.price.toString)
    )
    hbaseTable.put(put)
  }

  def write(prices: RDD[StockPrice]): Unit = {
    prices.map(price => {
      (price.symbol + "_" + (Long.MaxValue - price.time.toEpochMilli), price.price.toString)
    }).toHBaseTable(table.name)
      .toColumns("price:price")
      .save()
  }
  def write(prices: Seq[StockPrice]): Unit = write(sc.parallelize(prices))
}

object HBaseStockPriceDataSource {
  sealed trait Table { def name: String }
  case object GoogleFinance extends Table { override def name: String = "stockprices_google" }
  case object YahooFinance extends Table { override def name: String = "stockprices_yahoo" }
  case object WRDSTrades extends Table { override def name: String = "wrdstrades" }
  case object WRDSTradesMinuteRes extends Table { override def name: String = "wrdstrades_minuteres" }

  val priceCF: Array[Byte] = Bytes.toBytes("price")
  val priceCQ: Array[Byte] = Bytes.toBytes("price")
}
