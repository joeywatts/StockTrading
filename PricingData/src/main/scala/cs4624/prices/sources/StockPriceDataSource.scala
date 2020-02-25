package cs4624.prices.sources

import java.time.Instant

import cs4624.prices.StockPrice

trait StockPriceDataSource {
  def query(symbol: String,
            startTime: Option[Instant] = None,
            endTime: Option[Instant] = None): Iterator[StockPrice]

  def priceAtTime(symbol: String, time: Instant): Option[StockPrice]
}
