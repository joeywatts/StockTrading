package cs4624.microblog.sentiment.classification.svm

import cs4624.microblog.sentiment.classification.{ClassificationModel, Classifier}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by joeywatts on 3/1/17.
  */
case class SVMClassificationModel(model: SVMModel) extends ClassificationModel {

  override def classify(data: Vector) = {
    model.predict(data)
  }

  override def save(file: String)(implicit sc: SparkContext) = {
    model.save(sc, file)
  }

}

object SVMClassification extends Classifier {

  override def load(file: String)(implicit sc: SparkContext) = {
    val model = SVMModel.load(sc, file)
    Some(SVMClassificationModel(model))
  }

  override def train(labeledPoints: RDD[LabeledPoint])(implicit sc: SparkContext) = {
    val numIterations = 100
    val model = SVMWithSGD.train(labeledPoints, numIterations)
    model.clearThreshold()
    SVMClassificationModel(model)
  }

}
