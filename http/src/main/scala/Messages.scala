package com.eztier.rest.responses

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{DefaultJsonProtocol, JsString, JsValue, RootJsonFormat}

case class SearchMessage(mrn: String, name: String, dob: String)

object SearchMessageJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val detailedMessageFormat = jsonFormat3(SearchMessage)
}
