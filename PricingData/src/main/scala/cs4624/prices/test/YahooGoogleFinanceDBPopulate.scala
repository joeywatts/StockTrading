package cs4624.prices.test

import cs4624.common.App
import cs4624.common.Http.client
import cs4624.common.spark.SparkContextManager._
import cs4624.common.hbase.HBaseConnectionManager._

import cs4624.prices.EndOfDayStockQuote
import cs4624.prices.YahooFinanceAPI
import cs4624.prices.GoogleFinanceAPI
import cs4624.prices.sources.HBaseStockPriceDataSource
import cs4624.prices.sources.HBaseStockPriceDataSource.YahooFinance

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration

import java.time.LocalDate

object YahooGoogleFinanceDBPopulate extends App {
  val dataSource = new HBaseStockPriceDataSource(YahooFinance)

  val yahoo = new YahooFinanceAPI()
  val google = new GoogleFinanceAPI()

  val startDate = LocalDate.of(2015, 1, 1)
  val endDate = LocalDate.of(2015, 12, 31)

  val yahooSymbols = Seq("AAPL", "FB", "GILD", "KNDI", "MNKD", "NQ", "PLUG", "QQQ", "SPY", "TSLA", "^GSPC")
  val googleSymbols = Seq("VRNG")

  def toDailyPrices(prices: Seq[EndOfDayStockQuote]) =
    prices.flatMap(p => p.openStockPrice :: p.closeStockPrice :: Nil)

  val yahooPrices = yahooSymbols.map(symbol => yahoo.getQuotes(symbol, startDate, endDate))
  val googlePrices = googleSymbols.map(symbol => google.getQuotes(symbol, startDate, endDate))

  val future = for {
    quotes <- Future.sequence(yahooPrices ++ googlePrices)
  } yield dataSource.write(toDailyPrices(quotes.flatten))

  Await.ready(future, Duration.Inf)
}
