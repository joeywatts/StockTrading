package cs4624.trading.strategies

import cs4624.portfolio.Portfolio
import cs4624.prices.StockPrice
import cs4624.prices.sources.StockPriceDataSource
import cs4624.trading.{TradingEvent, TradingStrategy}
import java.time._

import cs4624.portfolio.error.TransactionError
import cs4624.trading.events._
import org.apache.log4j.LogManager

import scala.collection.mutable

class MovingAverageStrategy(stock: String,
  var portfolio: Portfolio,
  stockPriceDataSource: StockPriceDataSource,
  short: Int = 5, long: Int = 10) extends TradingStrategy {

  override def currentPortfolio = portfolio
  val log = LogManager.getLogger("MovingAverageStrategy")
  val prices = mutable.PriorityQueue[StockPrice]()(Ordering.by(-_.time.toEpochMilli))

  def runningAverage(n: Int): BigDecimal = {
    val p = prices.takeRight(n)
    p.map(_.price).sum / p.size
  }

  /**
    * Populate the prices queue.
    */
  def updatePrices(time: Instant): Unit = {
    if (prices.isEmpty) {
      val dates = mutable.Map[LocalDate, StockPrice]()
      stockPriceDataSource.query(
        symbol = stock,
        endTime = Some(time)
      ).forall(p => {
        val date = LocalDateTime.ofInstant(p.time, ZoneOffset.UTC).toLocalDate
        dates(date) = p
        dates.size <= long
      })
      prices ++= dates.values
    } else {
      stockPriceDataSource.priceAtTime(stock, time).foreach { p =>
        prices += p
      }
      while (prices.size > long) prices.dequeue
    }
  }
  def shortAverage = runningAverage(short)
  def longAverage = runningAverage(long)

  sealed trait Decision
  case object Buy extends Decision
  case object Sell extends Decision
  case object Wait extends Decision
  var decision: Decision = Wait

  override def on(event: TradingEvent): MovingAverageStrategy = {
    event match {
      case MarketOpen(time) =>
        portfolio = portfolio.withSplitAdjustments(time)
        updatePrices(time)
        decision match {
          case Buy => portfolio = handleTrade(
            portfolio.withSharesPurchasedAtValue(time, stock, portfolio.cash)
          )
          case Sell => portfolio = portfolio.withAllSharesSold(time)
          case Wait =>
        }
      case MarketClose(time) if prices.size >= long =>
        stockPriceDataSource.priceAtTime(stock, time).foreach { p =>
          val shortAverage = runningAverage(short)
          val longAverage = runningAverage(long)
          if (p.price > shortAverage && p.price > longAverage) {
            decision = Buy
          } else if (p.price < shortAverage && p.price < longAverage) {
            decision = Sell
          } else {
            decision = Wait
          }
        }
      case MarketClose(time) => println(prices.size)
      case _ =>
    }
    this
  }

  private def handleTrade(retValue: Either[Portfolio, TransactionError]): Portfolio = {
    retValue match {
      case Left(portfolio) => portfolio
      case Right(transactionError) =>
        log.error(transactionError.message)
        transactionError.portfolio
    }
  }

}
