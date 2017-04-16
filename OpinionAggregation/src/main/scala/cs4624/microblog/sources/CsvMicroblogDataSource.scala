package cs4624.microblog.sources
import java.time.Instant

import cs4624.common.{CSV, OptionalArgument}
import cs4624.microblog.sentiment.{Bearish, Bullish}
import cs4624.microblog.{MicroblogAuthor, MicroblogPost}

/**
  * Created by joeywatts on 3/13/17.
  */
class CsvMicroblogDataSource(csvFile: String)
  extends MicroblogDataSource {

  override def query(startTime: OptionalArgument[Instant],
                     endTime: OptionalArgument[Instant]): Iterator[MicroblogPost] = {
    scala.io.Source.fromFile(csvFile).getLines().map(CSV.parseLine).map(columns => {
      val author = MicroblogAuthor(columns(5))
      val sentiment = if (columns(9) == "Bearish") Some(Bearish)
      else if (columns(9) == "Bullish") Some(Bullish)
      else None
      MicroblogPost(
        id = columns(0),
        text = columns(2),
        author,
        time = Instant.parse(columns(4)),
        sentiment,
        symbols = columns(8).split("::::").toSet
      )
    })
  }

}
