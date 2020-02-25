package cs4624.portfolio

import org.scalatest._
import java.time._
import cs4624.prices._
import cs4624.prices.sources._
import cs4624.prices.splits._
import cs4624.prices.splits.sources._

class PortfolioSpec extends FlatSpec with Matchers with EitherValues {
  implicit val noSplits = new HardcodedStockSplitDataSource(Map())
  implicit val simplePrices = new HardcodedStockPriceDataSource(Seq(
    StockPrice("AAPL", Instant.MIN, BigDecimal("1.0"))
  ).groupBy(_.symbol))

  "A Portfolio" should "fail to buy a stock with no cash" in {
    println(simplePrices.priceAtTime("AAPL", Instant.now))
    val portfolio = Portfolio(Instant.now, BigDecimal("0.0"))
    val result = portfolio.withSharesPurchased(Instant.now, "AAPL", 1)
    result should be ('right)
    val result2 = portfolio.withSharesPurchasedAtValue(
      Instant.now, "AAPL", BigDecimal("5.0"))
    result2 should be ('right)
  }

  it should "fail to buy a stock when there's not enough cash to cover the transaction fee" in {
    val portfolio = Portfolio(Instant.now, BigDecimal("14.0"),
      transactionFee = BigDecimal("7.0"))
    val result = portfolio.withSharesPurchased(Instant.now, "AAPL", 9)
    result should be ('right)
  }

  it should "apply a transaction fee when purchasing a stock" in {
    val portfolio = Portfolio(Instant.now, BigDecimal("14.0"),
      transactionFee = BigDecimal("7.0"))
    val result = portfolio.withSharesPurchased(Instant.now, "AAPL", 1)
    result.left.value.cash should be (BigDecimal("6.0"))
  }

  it should "apply a transaction fee when selling a stock" in {
    val portfolio = Portfolio(Instant.now, BigDecimal("7.0"),
      Map("AAPL" -> 1), transactionFee = BigDecimal("7.0"))
    val result = portfolio.withSharesSold(Instant.now, "AAPL", 1)
    result.left.value.cash should be (BigDecimal("1.0"))
  }

  it should "fail to sell when the portfolio doesn't cover the transaction fee" in {
    val portfolio = Portfolio(Instant.now, BigDecimal("5.0"),
      Map("AAPL" -> 1), transactionFee = BigDecimal("7.0"))
    val result = portfolio.withSharesSold(Instant.now, "AAPL", 1)
    result should be ('right)
  }

  it should "successfully sell a stock" in {
    val portfolio = Portfolio(Instant.now, BigDecimal("6.0"),
      Map("AAPL" -> 1))
    val result = portfolio.withSharesSold(Instant.now, "AAPL", 1)
    result.left.value.cash should be (BigDecimal("7.0"))
  }
  it should "successfully buy a stock" in {
    val portfolio = Portfolio(Instant.now, BigDecimal("6.0"),
      Map("AAPL" -> 1))
    val result = portfolio.withSharesPurchased(Instant.now, "AAPL", 1)
    result.left.value.stocks("AAPL") should be (2)
    result.left.value.cash should be (BigDecimal("5.0"))
    val result2 = portfolio.withSharesPurchasedAtValue(Instant.now, "AAPL", BigDecimal("1.0"))
    result2.left.value.stocks("AAPL") should be (2)
    result2.left.value.cash should be (BigDecimal("5.0"))
  }

  it should "calculate portfolio value successfully" in {
    val portfolio = Portfolio(Instant.now, BigDecimal("6.0"),
      Map("AAPL" -> 1))
    portfolio.portfolioValue should be (BigDecimal("7.0"))
  }

  it should "buy or sell shares to get the target amount" in {
    val portfolio = Portfolio(Instant.now, BigDecimal("6.0"),
      Map("AAPL" -> 1))
    val result = portfolio.withSharesAtTargetAmount(Instant.now, "AAPL", 4)
    result.left.value.cash should be (BigDecimal("3.0"))
    result.left.value.stocks("AAPL") should be (4)
    val result2 = result.left.value.withSharesAtTargetValue(Instant.now, "AAPL", BigDecimal("3.0"))
    result2.left.value.cash should be (BigDecimal("4.0"))
    result2.left.value.stocks("AAPL") should be (3)
  }

  it should "fail to buy or sell shares without price data" in {
    val prices = new HardcodedStockPriceDataSource(Map())
    val portfolio = Portfolio(Instant.now, BigDecimal("6.0"),
      Map("AAPL" -> 1))(prices, noSplits)
    val buyResult = portfolio.withSharesPurchased(Instant.now, "AAPL", 4)
    buyResult should be ('right)
    val sellResult = portfolio.withSharesSold(Instant.now, "AAPL", 1)
    sellResult should be ('right)
  }

  it should "properly adjust stock amounts for stock splits" in {
    val splits = new HardcodedStockSplitDataSource(Seq(
      StockSplit("AAPL", LocalDate.of(2014, 1, 2), 2, 1)
    ).map { s => ((s.symbol, s.date), s) }.toMap)
    val portfolio = Portfolio(
      LocalDate.of(2014, 1, 1).atStartOfDay.toInstant(ZoneOffset.UTC),
      BigDecimal("6.0"),
      Map("AAPL" -> 1)
    )(simplePrices, splits)
    val result = portfolio.withSplitAdjustments(
      LocalDate.of(2014, 1, 3).atStartOfDay.toInstant(ZoneOffset.UTC)
    )
    result.stocks("AAPL") should be (2)
  }
  it should "properly adjust stock amounts for stock merges" in {
    val splits = new HardcodedStockSplitDataSource(Seq(
      StockSplit("AAPL", LocalDate.of(2014, 1, 2), 1, 2)
    ).map { s => ((s.symbol, s.date), s) }.toMap)
    val portfolio = Portfolio(
      LocalDate.of(2014, 1, 1).atStartOfDay.toInstant(ZoneOffset.UTC),
      BigDecimal("6.0"),
      Map("AAPL" -> 4)
    )(simplePrices, splits)
    val result = portfolio.withSplitAdjustments(
      LocalDate.of(2014, 1, 3).atStartOfDay.toInstant(ZoneOffset.UTC)
    )
    result.stocks("AAPL") should be (2)
  }
}
