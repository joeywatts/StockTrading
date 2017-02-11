package main

import main.Factories.{ClassifierType, FeatureGeneratorType}

/**
  * A singleton class that will serve as the entry point
  * Created by Eric on 2/1/2017.
  */
object PredictionEngine extends App{
  // this is the entry point for our application
  //println("Hello world")
  //val experiment = new Experiment(FeatureGeneratorType.Word2Vec, ClassifierType.LogisticRegression)
  StockDecisions.makeRun()
}