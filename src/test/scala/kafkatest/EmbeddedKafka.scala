package kafkatest

import java.io.IOException
import java.util.Properties
import kafka.server.KafkaConfig
import kafka.server.KafkaServerStartable

/**
  * Created by prayagupd
  * on 1/15/17.
  */

class EmbeddedKafka(zkProperties: Properties, kafkaProperties: Properties) {

  val kafkaServer = new KafkaServerStartable(new KafkaConfig(kafkaProperties))

  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def start() {

    println("========== starting local zookeeper =============")
    val zookeeperServer = new EmbeddedZooKeeper(zkProperties)
    zookeeperServer.start()
    println("zk done")

    Thread.sleep(1000)

    println("========== starting local kafka broker... =======")
    kafkaServer.startup()
    println("=========== kafka done ==========================")
  }

  def stop() {
    println("============== stopping kafka ===================")
    kafkaServer.shutdown()
    println("=============== done ============================")
  }

}