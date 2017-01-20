package kafkatest

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
  * Created by prayagupd
  * on 1/15/17.
  */

class EmbeddedZooKeeper(zkProperties: Properties) {

  @throws(classOf[FileNotFoundException])
  @throws(classOf[IOException])
  def start() {
    val quorumConfiguration = new QuorumPeerConfig()
    try {
      quorumConfiguration.parseProperties(zkProperties);
    } catch {
      case e: Throwable =>
        throw new RuntimeException(e)
    }

    val zooKeeperServer = new ZooKeeperServerMain();
    val configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);


    new Thread() {
      override def run() {
        try {
          zooKeeperServer.runFromConfig(configuration)
        } catch {
          case e: Throwable =>
            println("ZooKeeper Failed")
            e.printStackTrace(System.err)
        }
      }
    }.start()
  }
}
