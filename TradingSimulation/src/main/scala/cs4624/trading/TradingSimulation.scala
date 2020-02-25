package cs4624.trading

import java.io.PrintWriter
import java.time._
import java.time.temporal.ChronoUnit

import cs4624.trading.events.{MarketEventsEmitter, MarketOpen, StockPriceEventEmitter, MicroblogEventEmitter}
import cs4624.common.spark.SparkContextManager._
import cs4624.common.hbase.HBaseConnectionManager._
import cs4624.microblog.sentiment.SentimentAnalysisModel
import cs4624.microblog.sources.HBaseMicroblogDataSource
import cs4624.microblog.sources.HBaseMicroblogDataSource.Default
import cs4624.portfolio.Portfolio
import cs4624.prices.sources.HBaseStockPriceDataSource
import cs4624.prices.sources.HBaseStockPriceDataSource.YahooFinance
import cs4624.prices.splits.StockSplit
import cs4624.prices.splits.sources.HardcodedStockSplitDataSource
import cs4624.trading.strategies._
import cs4624.common.App
import cs4624.common.Http.client
import org.apache.log4j.LogManager

object TradingSimulation extends App {

  val log = LogManager.getLogger("TradingSimulation")

  val symbols = Set("AAPL", "FB", "GILD", "KNDI", "MNKD", "NQ", "PLUG", "QQQ", "SPY", "TSLA", "VRNG")
  implicit val hBaseStockPriceDataSource = new HBaseStockPriceDataSource(YahooFinance)
  implicit val stockSplitDataSource = new HardcodedStockSplitDataSource(Seq(
    StockSplit("AAPL", LocalDate.of(2014, 6, 9), 7, 1),
    StockSplit("VRNG", LocalDate.of(2015, 11, 30), 1, 10)
  ).map(split => ((split.symbol, split.date), split)).toMap)
  val hbaseMicroblogDataSource = new HBaseMicroblogDataSource(Default)

  SentimentAnalysisModel.load("../cs4624_sentiment_analysis_model3") match {
    case Some(sentimentAnalysisModel) =>
      val start = OffsetDateTime.of(2014, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant
      val end = OffsetDateTime.of(2015, 12, 31, 23, 59, 59, 999999999, ZoneOffset.UTC).toInstant
      val baselineStrategies = symbols.toSeq.map(s =>
        new BaselineStrategy(s, Portfolio(start, 100000, transactionFee = BigDecimal("6.0")),
          hBaseStockPriceDataSource)
      )
      val movingAverageStrategies = symbols.toSeq.map(s =>
        new MovingAverageStrategy(s, Portfolio(start, 100000, transactionFee = BigDecimal("6.0")),
          hBaseStockPriceDataSource)
      )
      val movingAverageWithSentimentStrategies = symbols.toSeq.map(s =>
        new MovingAverageWithSentimentStrategy(s, Portfolio(start, 100000, transactionFee = BigDecimal("6.0")),
          hBaseStockPriceDataSource)
      )
      val selectionBySentimentStrategy = new SelectionBySentimentStrategy(symbols, Portfolio(start, 100000 * symbols.size, transactionFee = BigDecimal("6.0")),
        hBaseStockPriceDataSource)
      val oneStockSelection = new SelectionBySentimentStrategy(symbols, Portfolio(start, 100000 * symbols.size, transactionFee = BigDecimal("6.0")),
        hBaseStockPriceDataSource, maxStocksToInvestIn = 1)
      val allStocksSelection = new SelectionBySentimentStrategy(symbols, Portfolio(start, 100000 * symbols.size, transactionFee = BigDecimal("6.0")),
        hBaseStockPriceDataSource, maxStocksToInvestIn = symbols.size)
      val indexFundStrategy = new BuyHoldStrategy("^GSPC", Portfolio(start, symbols.size * 100000.0, transactionFee = BigDecimal("6.0")))
      val strategies: Map[String, Seq[TradingStrategy]] = Map[String, Seq[TradingStrategy]](
        "Baseline" -> baselineStrategies,
        "MovingAverage" -> movingAverageStrategies,
        "MovingAverageWithSentiment" -> movingAverageWithSentimentStrategies,
        "SelectionBySentiment(OneStock)" -> Seq(oneStockSelection),
        "SelectionBySentiment" -> Seq(selectionBySentimentStrategy),
        "SelectionBySentiment(AllStocks)" -> Seq(allStocksSelection),
        "IndexFund" -> Seq(indexFundStrategy)
      )
      val eventSources = Seq(
        new MarketEventsEmitter(),
        new MicroblogEventEmitter(hbaseMicroblogDataSource, sentimentAnalysisModel)
      )
      val context = new TradingContext(strategies.values.flatten.toSeq, eventSources, start, end)
      val printWriter = new PrintWriter("../results.csv")
      val headerLine = strategies.keys.toSeq.sorted.foldLeft("Time") { case (line, name) => line + "," + name }
      printWriter.println(headerLine)
      log.info("Starting simulation....")
      context.run {
        case (MarketOpen(time), _) =>
          val portfolioValues = strategies.mapValues(_.map { _.currentPortfolio.portfolioValue }.sum)
          log.info(time)
          portfolioValues.foreach { case (name, value) =>
            log.info(s"$name -> $$$value")
          }
          val csvLine = portfolioValues.toList.sortBy { case (key, _) => key }.foldLeft("\"" + time + "\"") {
            case (line, (_, value)) => line + "," + value
          }
          printWriter.println(csvLine)
        case _ =>
      }
      printWriter.close()
    case None => println("Could not load sentiment analysis model!")
  }
}
