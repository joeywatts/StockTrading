package cs4624.trading.strategies

import cs4624.portfolio.Portfolio
import cs4624.portfolio.error.TransactionError
import cs4624.trading._
import cs4624.trading.events._
import cs4624.prices.StockPrice
import cs4624.prices.sources.StockPriceDataSource
import cs4624.microblog.sentiment._
import cs4624.microblog.aggregation.AggregatedOpinions

import scala.collection.mutable

import org.apache.log4j.LogManager

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._

import java.time._
import java.time.temporal.TemporalAmount

object MittalGoelStrategy {
  /**
    * A machine learning algorithm to predict next price given sentiment and last 3 days of prices.
    */
  class PricePredictionModel(model: LinearRegressionModel) {
    def predict(sentimentOpt: Option[Sentiment], first: StockPrice, second: StockPrice, third: StockPrice): Double = {
      val sentimentLabel = sentimentOpt match {
        case Some(sentiment) => sentiment.label
        case None => 0.5
      }
      val feature = Vectors.dense(sentimentLabel, first.price.toDouble, second.price.toDouble, third.price.toDouble)
      model.predict(feature)
    }
    def save(file: String)(implicit sc: SparkContext): Unit = {
      model.save(sc, file)
    }
  }
  object PricePredictionModel {
    def train(data: RDD[(Option[Sentiment], StockPrice, StockPrice, StockPrice, StockPrice)]): PricePredictionModel = {
      val processedData = data.map { case (sentimentOpt, first, second, third, nextDay) =>
        val sentimentLabel = sentimentOpt match {
          case Some(sentiment) => sentiment.label
          case None => 0.5
        }
        val feature = Vectors.dense(sentimentLabel, first.price.toDouble, second.price.toDouble, third.price.toDouble)
        LabeledPoint(nextDay.price.toDouble, feature)
      }
      val r = new LinearRegressionWithSGD()
      val model = r.run(processedData)
      new PricePredictionModel(model)
    }
    def load(file: String)(implicit sc: SparkContext): Option[PricePredictionModel] = {
      try {
        val model = LinearRegressionModel.load(sc, file)
        Some(new PricePredictionModel(model))
      } catch { case _: Throwable =>
        None
      }
    }
  }
}
class MittalGoelStrategy(stock: String,
  var portfolio: Portfolio,
  trainingPeriod: TemporalAmount)
  (implicit stockPriceDataSource: StockPriceDataSource,
    sc: SparkContext) extends TradingStrategy {

  sealed trait Status
  case object Training extends Status
  case object Running extends Status

  val aggregatedOpinions = new AggregatedOpinions(stockPriceDataSource, Duration.ofDays(1))
  val prices = mutable.ListBuffer[StockPrice]()

  private val k = 7
  private val n = 1
  private val m = 1

  private var startTime: Option[OffsetDateTime] = None
  private var priceAtBuyTime: Option[StockPrice] = None
  private var status: Status = Training
  private var pricePredictionModel: Option[MittalGoelStrategy.PricePredictionModel] = None

  private val log = LogManager.getLogger("Mittal-Goel Strategy")

  def runningAverage: Double = (prices.map(_.price).sum / prices.length).toDouble
  def runningStandardDeviation(avg: BigDecimal = runningAverage): Double =
    Math.sqrt(prices.map(_.price - avg).map(x => x * x).sum.toDouble / prices.length)

  override def currentPortfolio: Portfolio = portfolio

  val trainingData = mutable.ListBuffer[(Option[Sentiment], StockPrice, StockPrice, StockPrice, StockPrice)]()
  var previousSentiment: Option[Sentiment] = None
  var currentSentiment: Option[Sentiment] = None
  def trainingEventHandler(event: TradingEvent): TradingStrategy = {
    if (!startTime.isDefined) startTime = Some(OffsetDateTime.ofInstant(event.time, ZoneOffset.UTC))
    if (!event.time.isBefore(startTime.get.plus(trainingPeriod).toInstant)) {
      status = Running
      val rdd = sc.parallelize(trainingData)
      pricePredictionModel = Some(MittalGoelStrategy.PricePredictionModel.train(rdd))
      trainingData.clear()
    } else {
      event match {
        case MarketOpen(time) =>
          stockPriceDataSource.priceAtTime(stock, time) match {
            case Some(price) =>
              prices += price
              while (prices.length > k) prices.remove(0)
              previousSentiment = currentSentiment
              currentSentiment = aggregatedOpinions.sentimentForStock(stock)
              if (prices.length > 3) {
                trainingData += ((previousSentiment,
                  prices(prices.length - 4), prices(prices.length - 3),
                  prices(prices.length - 2), prices(prices.length - 1)))
              }
            case _ =>
          }
        case MicroblogPostEvent(post) if post.symbols.contains(stock) =>
          aggregatedOpinions.on(post)
        case _ =>
      }
    }
    this
  }
  def runningEventHandler(event: TradingEvent): TradingStrategy = {
    event match {
      case MarketOpen(time) =>
        stockPriceDataSource.priceAtTime(stock, time) match {
          case Some(price) =>
            prices += price
            while (prices.length > k) prices.remove(0)
	        portfolio = portfolio.withSplitAdjustments(time)
            val predictedPrice = pricePredictionModel.get.predict(
              aggregatedOpinions.sentimentForStock(stock),
              prices(prices.length - 3),
              prices(prices.length - 2),
              prices(prices.length - 1)
            )
            val runningAveragePrice = runningAverage
            val stddev = runningStandardDeviation(runningAveragePrice)
            if (predictedPrice > runningAveragePrice + n * stddev) {
              // Buy.
              priceAtBuyTime = Some(price)
              portfolio = handleTrade(portfolio.withSharesPurchasedAtValue(time, stock, portfolio.cash))
            }
            if (priceAtBuyTime.isDefined && predictedPrice < priceAtBuyTime.get.price - m * stddev) {
              // Sell.
              priceAtBuyTime = None
              portfolio = portfolio.withAllSharesSold(time)
            }
          case _ =>
        }
      case MicroblogPostEvent(post) if post.symbols.contains(stock) =>
        aggregatedOpinions.on(post)
      case _ =>
    }
    this
  }

  override def on(event: TradingEvent): TradingStrategy = {
    if (status == Training) trainingEventHandler(event)
    if (status == Running) runningEventHandler(event)
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
