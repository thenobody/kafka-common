package com.rightster.data.kafka

import java.util
import java.util.UUID

import com.rightster.data.kafka.MyJsonProtocol._
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import spray.json._

import scala.util.Random

/**
 * Created by antonvanco on 23/09/2015.
 */
object UUIDProducer {
  val logger = LoggerFactory.getLogger(getClass)

  class EventSerializer extends Serializer[Event] {
    def close(): Unit = {
      //
    }

    def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      //
    }

    def serialize(topic: String, data: Event): Array[Byte] = data.toJson.toString().getBytes("UTF8")
  }

  def main(args: Array[String]): Unit = {

    val producer = new Producer[Event, EventSerializer](List("localhost:9092"), "test", classOf[EventSerializer]) {
      override def getPartition(payload: Event, numPartitions: Int): Int = payload.id % numPartitions
    }
    while(true) {
      val message = Event(Math.abs(Random.nextInt() % 3), UUID.randomUUID().toString, System.currentTimeMillis(), payload = "asd")
      logger.info(s"sending $message")
      producer.send(message)
      Thread.sleep(100)
    }
    producer.close()
  }
}
