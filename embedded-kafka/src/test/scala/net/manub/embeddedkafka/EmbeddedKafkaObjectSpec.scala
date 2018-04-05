package net.manub.embeddedkafka

import java.net.ServerSocket

import net.manub.embeddedkafka.EmbeddedKafka._
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._
import scala.reflect.io.Directory

class EmbeddedKafkaObjectSpec extends EmbeddedKafkaSpecSupport {

  val consumerPollTimeout = 5000

  "the EmbeddedKafka object" when {
    "invoking the start and stop methods" should {
      "start and stop Kafka and Zookeeper on the default ports" in new TestContext {
        EmbeddedKafka.start()(someConfig)
        kafkaIsAvailable(kafkaPort)
        zookeeperIsAvailable(zookeeperPort)

        EmbeddedKafka.stop()

        kafkaIsNotAvailable()
        zookeeperIsNotAvailable()
      }

      "start and stop Kafka and Zookeeper on different specified ports using an implicit configuration" in new TestContext {
        EmbeddedKafka.start()(someConfig)

        kafkaIsAvailable(kafkaPort)
        zookeeperIsAvailable(zookeeperPort)

        EmbeddedKafka.stop()
      }

      "start and stop a specific Kafka" in new TestContext {
        val firstBroker = EmbeddedKafka.start()(someConfig)
        EmbeddedKafka.start()(someOtherConfig)

        kafkaIsAvailable(kafkaPort)
        zookeeperIsAvailable(zookeeperPort)

        kafkaIsAvailable(anotherKafkaPort)
        zookeeperIsAvailable(anotherZookeeperPort)

        EmbeddedKafka.stop(firstBroker)

        kafkaIsNotAvailable(kafkaPort)
        zookeeperIsNotAvailable(zookeeperPort)

        kafkaIsAvailable(anotherKafkaPort)
        zookeeperIsAvailable(anotherZookeeperPort)

        EmbeddedKafka.stop()
      }

      "start and stop multiple Kafka instances on specified ports" in new TestContext {
        val someBroker = EmbeddedKafka.start()(someConfig)

        val someOtherBroker = EmbeddedKafka.start()(someOtherConfig)

        val topic = "publish_test_topic_1"
        val someOtherMessage = "another message!"

        val serializer = new StringSerializer
        val deserializer = new StringDeserializer

        publishToKafka(topic, "hello world!")(someConfig, serializer)
        publishToKafka(topic, someOtherMessage)(someOtherConfig, serializer)

        kafkaIsAvailable(someConfig.kafkaPort)
        EmbeddedKafka.stop(someBroker)

        val anotherConsumer = kafkaConsumer(someOtherConfig, deserializer, deserializer)
        anotherConsumer.subscribe(List(topic).asJava)

        val moreRecords = anotherConsumer.poll(consumerPollTimeout)
        moreRecords.count shouldBe 1

        val someOtherRecord = moreRecords.iterator().next
        someOtherRecord.value shouldBe someOtherMessage

        EmbeddedKafka.stop()
      }
    }

    "invoking the isRunning method" should {
      "return true when both Kafka and Zookeeper are running" in new TestContext {
        EmbeddedKafka.start()(someConfig)
        EmbeddedKafka.isRunning shouldBe true
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }

      "return true when only Kafka is running" in new TestContext {
        val unmanagedZookeeper = EmbeddedKafka.startZooKeeper(randomAvailablePort, Directory.makeTemp("zookeeper-test-logs"))

        EmbeddedKafka.startKafka(Directory.makeTemp("kafka-test-logs"))(someConfig)
        EmbeddedKafka.isRunning shouldBe true
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false

        unmanagedZookeeper.shutdown()
      }

      "return false when only Zookeeper is running" in new TestContext {
        EmbeddedKafka.startZooKeeper(Directory.makeTemp("zookeeper-test-logs"))(someConfig)
        EmbeddedKafka.isRunning shouldBe false
        EmbeddedKafka.stop()
        EmbeddedKafka.isRunning shouldBe false
      }
    }
  }

  private class TestContext {
    val kafkaPort = randomAvailablePort
    val zookeeperPort = randomAvailablePort

    val anotherKafkaPort = randomAvailablePort
    val anotherZookeeperPort = randomAvailablePort

    val someConfig = EmbeddedKafkaConfig(kafkaPort = kafkaPort, zooKeeperPort = zookeeperPort)
    val someOtherConfig = EmbeddedKafkaConfig(kafkaPort = anotherKafkaPort, zooKeeperPort = anotherZookeeperPort)

    def randomAvailablePort: Int = new ServerSocket(0).getLocalPort
  }
}
