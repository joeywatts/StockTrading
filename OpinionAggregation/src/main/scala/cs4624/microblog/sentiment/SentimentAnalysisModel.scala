package cs4624.microblog.sentiment

import cs4624.microblog.MicroblogPost
import cs4624.microblog.sentiment.classification.logisticregression.LogisticRegressionClassification
import cs4624.microblog.sentiment.classification.svm.SVMClassification
import cs4624.microblog.sentiment.classification.{ClassificationModel, Classifier}
import cs4624.microblog.sentiment.featureextraction.word2vec.Word2VecFeatureExtraction
import cs4624.microblog.sentiment.featureextraction.{FeatureExtractionModel, FeatureExtractor}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.{MulticlassMetrics, MultilabelMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by joeywatts on 3/1/17.
  */
case class SentimentAnalysisModel(featureExtractor: FeatureExtractionModel, classifier: ClassificationModel) {
  def predict(post: MicroblogPost): Option[Double] = {
    featureExtractor.extract(post).map(classifier.classify)
  }
  /**
    * Predict the sentiment of posts.
    * @param posts - the posts to analyze.
    * @return an RDD of posts with their sentiments.
    */
  def predict(posts: RDD[MicroblogPost]): RDD[(MicroblogPost, Double)] = {
    posts.flatMap(p => { featureExtractor.extract(p).map(vector => (p, classifier.classify(vector))) })
  }
  def save(filePrefix: String)(implicit sc: SparkContext) = {
    featureExtractor.save(s"$filePrefix/feature-extractor")
    classifier.save(s"$filePrefix/classifier")
  }
  def evaluate(posts: RDD[MicroblogPost]): SentimentAnalysisMetrics = {
    val predictions = predict(posts)
    val labels = predictions.map { case (post, predictedLabel) => (post.sentiment.get.label, predictedLabel) }
    val multiclassMetrics = new MulticlassMetrics(labels)
    val multilabelMetrics = new MultilabelMetrics(labels.map { case (trueLabel, predictedLabel) =>
      (Array(trueLabel), Array(predictedLabel))
    })
    SentimentAnalysisMetrics(multiclassMetrics, multilabelMetrics)
  }
}

object SentimentAnalysisModel {
  def apply(trainingData: RDD[MicroblogPost],
            featureExtractor: FeatureExtractor = Word2VecFeatureExtraction,
            classifier: Classifier = LogisticRegressionClassification)
           (implicit sc: SparkContext): SentimentAnalysisModel = {
    val filteredTrainingData = trainingData.filter(_.sentiment.isDefined)
    val featureExtractionModel= featureExtractor.train(filteredTrainingData)
    val labeledPoints = filteredTrainingData.flatMap(p =>
      featureExtractionModel.extract(p) match {
        case Some(vector) => Some(LabeledPoint(p.sentiment.get.label, vector))
        case None => None
      }
    )
    val classificationModel = classifier.train(labeledPoints)
    SentimentAnalysisModel(featureExtractionModel, classificationModel)
  }

  def load(filePrefix: String,
           featureExtractor: FeatureExtractor = Word2VecFeatureExtraction,
           classifier: Classifier = LogisticRegressionClassification)
          (implicit sc: SparkContext): Option[SentimentAnalysisModel] = {
    (featureExtractor.load(s"$filePrefix/feature-extractor"), classifier.load(s"$filePrefix/classifier")) match {
      case (Some(featureExtractionModel), Some(classificationModel)) =>
        Some(SentimentAnalysisModel(featureExtractionModel, classificationModel))
      case _ => None
    }
  }
}
