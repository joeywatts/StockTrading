package cs4624.trading.strategies

import cs4624.microblog.aggregation.AggregatedOpinions
import cs4624.microblog.sentiment.{Bearish, Bullish, SentimentAnalysisModel}
import cs4624.portfolio.Portfolio
import cs4624.prices.sources.StockPriceDataSource
import cs4624.trading.{TradingEvent, TradingStrategy}
import java.time.{Duration, Instant}

import cs4624.portfolio.error.TransactionError
import cs4624.trading.events._
import org.apache.log4j.LogManager

/**
  * Created by joeywatts on 3/15/17.
  */
class SelectionBySentimentStrategy(stocks: Set[String],
  var portfolio: Portfolio,
  stockPriceDataSource: StockPriceDataSource,
  maxStocksToInvestIn: Int = 3) extends TradingStrategy {
  
  val aggregatedOpinions = new AggregatedOpinions(stockPriceDataSource, Duration.ofDays(1))
  private val log = LogManager.getLogger("SelectionBySentiment")

  override def currentPortfolio: Portfolio = portfolio

  override def on(event: TradingEvent): SelectionBySentimentStrategy = {
    event match {
      case MicroblogPostEvent(post) if post.symbols.intersect(stocks).nonEmpty =>
        aggregatedOpinions.on(post)
      case MarketOpen(time) =>
        portfolio = portfolio.withSplitAdjustments(time)
        val stockOpinions = stocks.map { s => (s, aggregatedOpinions.opinionForStock(s)) }
        val sentiments = stockOpinions.groupBy { case (_, opinion) =>
          aggregatedOpinions.sentimentForOpinion(opinion)
        }
        sentiments.get(Some(Bullish)).foreach { opinions =>
          // Take the top Bullish stocks, and invest all in them.
          val buy = opinions.toSeq
            .sortBy { case (stock, opinion) => -opinion }
            .map(_._1)
            .take(maxStocksToInvestIn)
            .sortBy(-portfolio.stocks(_))
          val sell = stocks diff buy.toSet
          sell.foreach { stock =>
            portfolio = handleTrade(
              portfolio.withSharesAtTargetAmount(time, stock, 0)
            )
          }
          buy.foreach { stock =>
            portfolio = handleTrade(
              portfolio.withSharesAtTargetValue(time, stock, portfolio.portfolioValue / buy.size)
            )
          }
        }
        aggregatedOpinions.reset()
        log.info(portfolio)
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
