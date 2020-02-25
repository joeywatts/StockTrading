package cs4624.common.spark

import org.apache.spark.{SparkConf, SparkContext}
import cs4624.common.hbase.HBaseConnectionManager

/**
  * Created by ericrw96 on 2/2/17.
  */
object SparkContextManager {

  // change once it is running on the cluster
  private val conf = new SparkConf()
    .setAppName("StockTrading")
    .set("spark.master", "spark://master:7077")
    .set("spark.hbase.host", HBaseConnectionManager.ZookeeperQuorum)
    .set("spark.default.parallelism", "16")

  implicit val sc = new SparkContext(conf)

  def getContext: SparkContext = sc
}
