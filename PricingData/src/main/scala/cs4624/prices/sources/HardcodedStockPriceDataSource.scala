package cs4624.prices.sources

import java.time.Instant

import cs4624.prices.StockPrice

class HardcodedStockPriceDataSource(prices: Map[String, Seq[StockPrice]])
    extends StockPriceDataSource {
  def query(symbol: String,
    startTime: Option[Instant],
    endTime: Option[Instant]): Iterator[StockPrice] = {

    prices.getOrElse(symbol, Seq())
      .filter(!_.time.isBefore(startTime.getOrElse(Instant.MIN)))
      .filter(!_.time.isAfter(endTime.getOrElse(Instant.MAX)))
      .sortBy(_.time.toEpochMilli)
      .iterator
  }

  def priceAtTime(symbol: String, time: Instant): Option[StockPrice] = {
    query(symbol, endTime = Some(time)).toSeq.reverse.headOption
  }
}
