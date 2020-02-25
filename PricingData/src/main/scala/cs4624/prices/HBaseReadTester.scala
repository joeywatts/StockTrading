package cs4624.prices

import org.apache.hadoop.hbase.client.ConnectionFactory
import java.time.{OffsetDateTime, ZoneOffset}

import cs4624.common.spark.SparkContextManager._
import cs4624.prices.sources.HBaseStockPriceDataSource
import cs4624.prices.sources.HBaseStockPriceDataSource.YahooFinance

/**
  * Created by joeywatts on 2/12/17.
  */
object HBaseReadTester extends App {

  implicit val connection = ConnectionFactory.createConnection()
  val priceDataSource = new HBaseStockPriceDataSource(YahooFinance)
  val endTime = OffsetDateTime.of(2015, 5, 12, 1, 0, 0, 0, ZoneOffset.UTC).toInstant
  println(endTime)
  //val prices = priceDataSource.query("AAPL", startTime = OffsetDateTime.of(2014, 2, 10, 0, 0, 0, 0, ZoneOffset.UTC).toInstant, endTime = endTime)
  //prices.foreach(println)
  val price = priceDataSource.priceAtTime("AAPL", endTime)
  println(price)
}
