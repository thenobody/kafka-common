package net.thenobody.kafka

import java.util.concurrent.Executors
import java.util.{Properties, UUID}

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.serializer.StringDecoder
import org.slf4j.LoggerFactory

/**
 * Created by antonvanco on 23/09/2015.
 */
object UUIDConsumer {

  val logger = LoggerFactory.getLogger(getClass)

  class ConsumerTest(val id: String, val messageStream: KafkaStream[String, String]) extends Runnable {
    def run(): Unit = {
      val iterator = messageStream.iterator()
      Iterator.continually(iterator.next()).takeWhile(_ => iterator.hasNext()).foreach { messageAndMetadata =>
        val message = messageAndMetadata.message()
        logger.info(s"consumer $id received message: $message")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "test-consumer-group")
    props.put("zookeeper.session.timeout.ms", "1000")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")

    val executorService = Executors.newFixedThreadPool(3)

    val consumerConfig = new ConsumerConfig(props)
    val consumer = Consumer.create(consumerConfig)
    consumer.createMessageStreams(Map("test" -> 1), new StringDecoder, new StringDecoder).foreach { case (topic, streams) =>
      logger.info(s"consuming topic $topic")
      streams.foreach { stream =>
        executorService.submit(new ConsumerTest(UUID.randomUUID().toString, stream))
      }
    }
  }

}
