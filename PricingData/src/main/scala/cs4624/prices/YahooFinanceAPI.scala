package cs4624.prices

import play.api.libs.ws.WSClient
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import java.time.LocalDate
import java.net.URLDecoder
import java.nio.charset.Charset

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by joeywatts on 2/6/17.
  */
class YahooFinanceAPI(implicit ws: WSClient) extends EODStockQuoteAPI {
  implicit val quoteReads: Reads[EndOfDayStockQuote] = ((__ \ "Symbol").read[String].map { URLDecoder.decode(_, Charset.defaultCharset().name()) } and
    (__ \ "Date").read[LocalDate] and
    (__ \ "Open").read[String].map { BigDecimal(_) } and
    (__ \ "High").read[String].map { BigDecimal(_) } and
    (__ \ "Low").read[String].map { BigDecimal(_) } and
    (__ \ "Close").read[String].map { BigDecimal(_) } and
    (__ \ "Volume").read[String].map { _.toLong } and
    (__ \ "Adj_Close").read[String].map { BigDecimal(_) })(EndOfDayStockQuote.apply _)

  override def getQuotes(symbol: String, startDate: LocalDate, endDate: LocalDate)(implicit ec: ExecutionContext): Future[Seq[EndOfDayStockQuote]] = {
    val yqlString = "select * from yahoo.finance.historicaldata where symbol = \"" + symbol +
      "\" and startDate = \"" + startDate + "\" and endDate = \"" + endDate + "\""
    ws.url ("http://query.yahooapis.com/v1/public/yql").withQueryString (
      "q" -> yqlString,
      "format" -> "json",
      "env" -> "store://datatables.org/alltableswithkeys",
      "callback" -> ""
    ).get ().map (resp => {
      (resp.json \ "query" \ "results" \ "quote").asOpt[Seq[EndOfDayStockQuote]] getOrElse Seq()
    })
  }
}
