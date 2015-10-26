package net.thenobody.kafka

import spray.json.DefaultJsonProtocol

/**
 * Created by antonvanco on 23/09/2015.
 */
object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val eventFormat = jsonFormat4(Event)
}

case class Event(id: Int, uuid: String, timestamp: Long, payload: String)
