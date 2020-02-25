package cs4624.microblog.sources

import java.time.Instant

import cs4624.microblog.MicroblogPost
import org.apache.spark.rdd.RDD

/**
  * Created by joeywatts on 2/28/17.
  */
trait MicroblogDataSource {
  def query(startTime: Option[Instant] = None,
            endTime: Option[Instant] = None): Iterator[MicroblogPost]
}
