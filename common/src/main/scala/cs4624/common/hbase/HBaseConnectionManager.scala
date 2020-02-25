package cs4624.common.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._

object HBaseConnectionManager {

  val ZookeeperQuorum = "zookeeper"

  private def configuration = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", ZookeeperQuorum)
    conf
  }

  implicit lazy val connection = ConnectionFactory.createConnection(
    configuration
  )

}
