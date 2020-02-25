package cs4624.trading.events

import java.time.Instant

import cs4624.common.spark.RDDUtils
import cs4624.prices.StockPrice
import cs4624.prices.sources.StockPriceDataSource
import cs4624.trading.{TradingEvent, TradingEventEmitter}

case class StockPriceEvent(stockPrice: StockPrice) extends TradingEvent {
  override def time = stockPrice.time
  def symbol = stockPrice.symbol
  def price = stockPrice.price
}

class StockPriceEventEmitter(val symbol: String, val stockPriceDataSource: StockPriceDataSource) extends TradingEventEmitter {
  override def eventsForInterval(start: Instant, end: Instant): Iterator[TradingEvent] = {
    stockPriceDataSource.query(symbol, Some(start), Some(end)).map(StockPriceEvent)
  }
}
