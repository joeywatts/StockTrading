package cs4624.trading.events

import java.time.Instant

import cs4624.microblog.MicroblogPost
import cs4624.microblog.sources.MicroblogDataSource
import cs4624.trading.{TradingEvent, TradingEventEmitter}
import cs4624.microblog.sentiment._

case class MicroblogPostEvent(post: MicroblogPost) extends TradingEvent {
  override def time = post.time
}

class MicroblogEventEmitter(microblogDataSource: MicroblogDataSource,
  sentimentAnalysisModel: SentimentAnalysisModel) extends TradingEventEmitter {

  override def eventsForInterval(start: Instant, end: Instant) = {
    microblogDataSource.query(startTime = Some(start), endTime = Some(end))
      .map { post =>
        if (post.sentiment.isDefined) post
        else post.copy(sentiment = sentimentAnalysisModel.predict(post).flatMap(Sentiment.fromLabel))
      }.map(MicroblogPostEvent)
  }

}
