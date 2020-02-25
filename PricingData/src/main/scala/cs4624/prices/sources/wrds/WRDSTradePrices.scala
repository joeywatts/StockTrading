package cs4624.prices.sources.wrds

import java.time._
import java.time.format._

import cs4624.common.CSV
import cs4624.prices.StockPrice

import scala.io.Source

object WRDSTradePrices {
  val timeFormatter = DateTimeFormatter.ofPattern("H:mm:ss.SSS")

  def fromCSV(fileName: String): Iterator[StockPrice] = {
    val lines = Source.fromFile(fileName).getLines
    if (lines.hasNext) {
      val header = CSV.parseLine(lines.next).zipWithIndex.toMap
      lines.map(line => {
        val parsed = CSV.parseLine(line)
        val date = parsed(header("DATE"))
        val time = parsed(header("TIME_M"))
        val symbol = parsed(header("SYM_ROOT"))
        val price = parsed(header("PRICE"))
        val parsedDate = LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE)
        val parsedTime = LocalTime.parse(time, timeFormatter)
        val dateTime = OffsetDateTime.of(parsedDate, parsedTime, ZoneOffset.UTC)
        StockPrice(symbol, dateTime.toInstant, BigDecimal(price))
      })
    } else {
      Seq[StockPrice]().iterator
    }
  }
}
