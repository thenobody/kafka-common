package net.thenobody.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}

/**
 * Created by antonvanco on 24/09/2015.
 */
class Producer[V, S <: Serializer[V]](
  val brokers: List[String],
  val topic: String,
  val serializer: Class[S],
  val properties: Properties = new Properties()
) extends KafkaProducer[Null, V]({
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers.mkString(","))
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer.getName)
  properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, Producer.DefaultCompression)
  properties
}) {

  def getPartition(payload: V, numPartitions: Int): Int = payload.hashCode() % numPartitions

  def send(payload: V): Unit = {
    val partition = getPartition(payload, partitionsFor(topic).size())
    val record = new ProducerRecord[Null, V](topic, partition, null, payload)
    send(record)
  }
}

object Producer {
  val DefaultCompression = "lz4"
}
