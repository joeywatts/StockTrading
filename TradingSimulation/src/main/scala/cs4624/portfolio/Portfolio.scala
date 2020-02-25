package cs4624.portfolio
import java.time._

import cs4624.portfolio.error.TransactionError
import cs4624.prices.sources.StockPriceDataSource
import cs4624.prices.splits.StockSplit
import cs4624.prices.splits.sources.StockSplitDataSource

/**
  * A class to represent a snapshot of your portfolio.
  * @param time - the time of the portfolio
  * @param cash - the amount of cash you can use to purchase stocks
  * @param stocks - a mapping of stock symbol to the amount that you currently own.
  */
case class Portfolio(time: Instant, cash: BigDecimal,
  stocks: Map[String, Int] = Map.empty.withDefaultValue(0),
  transactionFee: BigDecimal = BigDecimal("0.0"))
  (implicit stockPrices: StockPriceDataSource,
    stockSplits: StockSplitDataSource) {

  private def currentStockPrice(symbol: String, time: Instant = this.time): Option[BigDecimal] = {
    stockPrices.priceAtTime(symbol, time).map(_.price)
  }

  /**
    * @return total value of this portfolio (sum of cash and all owned stock values)
    */
  def portfolioValue: BigDecimal = stocks.foldLeft(cash) { case (value, (symbol, amount)) =>
    value + currentStockPrice(symbol).getOrElse(BigDecimal("0.0")) * amount
  }

  def withTime(time: Instant): Portfolio = copy(time = time)

  def withSplitAdjustments(time: Instant): Portfolio = {
    val startDate = OffsetDateTime.ofInstant(this.time, ZoneOffset.UTC).plusDays(1).toLocalDate
    val endDate = OffsetDateTime.ofInstant(time, ZoneOffset.UTC).toLocalDate
    var date = startDate
    var result = copy(time = time)
    while (!date.isAfter(endDate)) {
      val splits = stocks.keys.flatMap(stockSplits.get(_, date))
      result = splits.foldLeft(result) { (portfolio, split) => portfolio.withSplitAdjustment(split) }
      date = date.plusDays(1)
    }
    result
  }

  def withSplitAdjustment(split: StockSplit): Portfolio = stocks.get(split.symbol) match {
    case Some(amount) =>
      val amountBefore = amount / split.beforeAmount
      val amountAfterSplit = amountBefore * split.afterAmount
      val fractionalShares = amount - amountBefore
      val stockPriceBeforeSplitOpt = currentStockPrice(split.symbol, split.date.atStartOfDay().atOffset(ZoneOffset.UTC).minusSeconds(1).toInstant)
      stockPriceBeforeSplitOpt match {
        case Some(stockPriceBeforeSplit) =>
          val cashValueOfFractionalShares = stockPriceBeforeSplit * fractionalShares
          println(s"Performed $$${split.symbol} split: $amountBefore -> $amountAfterSplit")
          this.copy(
            cash = this.cash + cashValueOfFractionalShares,
            stocks = this.stocks.updated(split.symbol, amountAfterSplit)
          )
        case None =>
          println(s"No price found for $$${split.symbol} (before split)")
          this
      }
    case None => this
  }

  /**
    * Set the amount of shares for a symbol in this portfolio (selling/buying shares as needed to achieve that amount).
    * @param time - the time of this event.
    * @param symbol - the stock symbol
    * @param targetAmount - the amount of shares to own
    * @return a Portfolio with this transaction completed or an error detailing why the transaction did not occur.
    */
  def withSharesAtTargetAmount(time: Instant, symbol: String, targetAmount: Int): Either[Portfolio, TransactionError] = {
    val result = this.withTime(time)
    val amountDifferential = targetAmount - stocks(symbol)
    if (amountDifferential == 0) Left(result)
    else if (targetAmount < 0)
      Right(TransactionError(result, s"Attempted to sell shares of $$$symbol that are not owned."))
    else
      currentStockPrice(symbol, time = time) match {
        case Some(stockPrice) =>
          val cashDifferential = amountDifferential * stockPrice
          val cashAfterTransaction = cash - cashDifferential - transactionFee
          if (cashAfterTransaction < 0)
            Right(TransactionError(result, s"Not enough cash remaining to buy $amountDifferential shares of $$$symbol."))
          else
            Left(
              result.copy(
                cash = cashAfterTransaction,
                stocks = stocks.updated(symbol, targetAmount)
              )
            )
        case None => Right(TransactionError(result, s"No price found for $$$symbol."))
      }
  }

  def withSharesAtTargetValue(time: Instant, symbol: String, totalValue: BigDecimal): Either[Portfolio, TransactionError] = {
    if (totalValue < transactionFee) {
      Left(this)
    } else {
      currentStockPrice(symbol, time = time) match {
        case Some(stockPrice) =>
          withSharesAtTargetAmount(time, symbol, ((totalValue - transactionFee) / stockPrice).toInt)
        case None => Right(TransactionError(this, s"No price found for $$$symbol."))
      }
    }
  }

  def withSharesPurchased(time: Instant, symbol: String, buyAmount: Int): Either[Portfolio, TransactionError] = {
    withSharesAtTargetAmount(time, symbol, stocks(symbol) + buyAmount)
  }

  def withSharesPurchasedAtValue(time: Instant, symbol: String, buyValue: BigDecimal): Either[Portfolio, TransactionError] = {
    if (buyValue < transactionFee) {
      Left(this)
    } else {
      currentStockPrice(symbol, time = time) match {
        case Some(stockPrice) =>
          withSharesAtTargetAmount(time, symbol, stocks(symbol) + ((buyValue - transactionFee) / stockPrice).toInt)
        case None => Right(TransactionError(this, s"No price found for $$$symbol."))
      }
    }
  }

  def withSharesSold(time: Instant, symbol: String, sellAmount: Int): Either[Portfolio, TransactionError] = {
    withSharesAtTargetAmount(time, symbol, stocks(symbol) - sellAmount)
  }

  def withAllSharesSold(time: Instant): Portfolio = {
    val numberOfSecuritiesOwned = stocks.filter { case (_, count) => count > 0 }.size
    this.withTime(time).copy(
      cash = portfolioValue - numberOfSecuritiesOwned * transactionFee,
      stocks = Map.empty.withDefaultValue(0)
    )
  }

  /**
    * Get the current value of the shares held for a given symbol.
    * @param symbol - the stock symbol.
    * @return the value of all of the shares in the portfolio with this symbol.
    */
  def stockValue(symbol: String): BigDecimal = {
    currentStockPrice(symbol).getOrElse(BigDecimal("0.0")) * stocks(symbol)
  }
}
