package cs4624.microblog

import cs4624.common.App

import cs4624.microblog.sentiment.{Bearish, Bullish, SentimentAnalysisModel}
import cs4624.microblog.sources.{CsvMicroblogDataSource, SparkHBaseMicroblogDataSource}
import cs4624.microblog.sources.SparkHBaseMicroblogDataSource.Default
import cs4624.microblog.sentiment.classification.svm.SVMClassification

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

/**
  * Created by joeywatts on 3/1/17.
  */
object Test extends App {

  import cs4624.common.spark.SparkContextManager._

  val dataSource = new SparkHBaseMicroblogDataSource(Default)

  //val csvDataSource = new CsvMicroblogDataSource("../../2014_stocktwits_data_11firms.csv")
  //val posts = csvDataSource.query()
  //dataSource.write(posts)

  val posts = dataSource.queryRDD()
  //println("Post count: " + posts.count())
  val postsWithSentiment = posts.filter(_.sentiment.isDefined)
  val seed = System.currentTimeMillis
  val N = 500000
/*  val bearishData = postsWithSentiment.filter(_.sentiment == Some(Bearish))
  val bullishData = postsWithSentiment.filter(_.sentiment == Some(Bullish))
  val bullishTrainingData = bullishData.sample(withReplacement = false, 0.8, seed)
  val bearishTrainingData = bearishData.sample(withReplacement = false, 0.8, seed)*/
  //val bullishTestingData = bullishData subtract bullishTrainingData
  //val bearishTestingData = bearishData subtract bearishTrainingData
  val sample = postsWithSentiment.sample(withReplacement = false, 0.6, seed)
  sample.cache()
  val model = SentimentAnalysisModel(sample,
    classifier = SVMClassification)
  model.save("../cs4624_sentiment_analysis_model4")
  /*val test = model.predict(bullishTestingData union bearishTestingData).map {
    case (post, predictedLabel) =>
      (post.sentiment.get.label, predictedLabel)
  }
  val metrics = new BinaryClassificationMetrics(test)
  println("Area under ROC = " + metrics.areaUnderROC())*/
}
