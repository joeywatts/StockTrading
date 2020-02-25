package cs4624.trading.events

import scala.concurrent._

import java.time._

import cs4624.common.Http
import cs4624.prices.{EODStockQuoteAPI, YahooFinanceAPI}
import cs4624.trading.{TradingEvent, TradingEventEmitter}

/**
  * Events to react to the market open and close.
  */
case class MarketOpen(override val time: Instant) extends TradingEvent
case class MarketClose(override val time: Instant) extends TradingEvent

class MarketEventsEmitter(eodStockQuoteAPI: EODStockQuoteAPI = new YahooFinanceAPI()(Http.client)) extends TradingEventEmitter {

  val openTime = OffsetTime.of(8, 0, 0, 0, ZoneOffset.UTC)
  val closeTime = OffsetTime.of(17, 0, 0, 0, ZoneOffset.UTC)

  override def eventsForInterval(start: Instant, end: Instant): Iterator[TradingEvent] = {
    val startDateTime = start.atOffset(ZoneOffset.UTC)
    val endDateTime = end.atOffset(ZoneOffset.UTC)
    val startDate = startDateTime.toLocalDate
    val endDate = endDateTime.toLocalDate
    import scala.concurrent.duration._
    import scala.concurrent.ExecutionContext.Implicits.global
    val years = startDate.getYear to endDate.getYear
    val future = Future.sequence(years.map { year =>
      eodStockQuoteAPI.getQuotes("DOW",
        LocalDate.of(year, 1, 1), LocalDate.of(year, 12, 31)).map { quotes =>

        val events: Seq[TradingEvent] = quotes.map(_.date).flatMap(date => {
          MarketOpen(date.atTime(openTime).toInstant) ::
          MarketClose(date.atTime(closeTime).toInstant) :: Nil
        })
        events.dropWhile(_.time.isBefore(start))
          .reverse.dropWhile(_.time.isAfter(end)).reverse
      }
    })
    val events = Await.result(future, Duration.Inf)
    events.flatten.sortBy(_.time.toEpochMilli).iterator
  }
}
