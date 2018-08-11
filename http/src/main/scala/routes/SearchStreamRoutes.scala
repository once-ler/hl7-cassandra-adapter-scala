package com.eztier.rest.routes

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.common
import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.server.Directives._
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.util.ByteString
import com.eztier.rest.responses.SearchMessage
import com.eztier.rest.responses.SearchMessageJsonProtocol._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait SearchStreamRoutes {
  implicit val actorSystem: ActorSystem
  implicit val streamMaterializer: ActorMaterializer
  implicit val executionContext: ExecutionContext
  lazy val httpStreamingRoutes =
    streamingJsonRoute

  /*
  // Configure the EntityStreamingSupport to render the elements as:
  // {"example":42}
  // {"example":43}
  // ...
  // {"example":1000}
  val start = ByteString.empty
  val sep = ByteString("\n")
  val end = ByteString.empty

  implicit val jsonStreamingSupport = EntityStreamingSupport.json()
    .withFramingRenderer(Flow[ByteString].intersperse(start, sep, end))
  */

  // Note that the default support renders the Source as JSON Array [{},{}]
  implicit val jsonStreamingSupport: akka.http.scaladsl.common.JsonEntityStreamingSupport = EntityStreamingSupport.json()

  // More customization:
  // https://doc.akka.io/docs/akka-http/current/routing-dsl/source-streaming-support.html?language=scala#Customising_response_rendering_mode
  def streamingJsonRoute =
    path("streaming-json") {
      get {
        val sourceOfNumbers = Source(1 to 15)
        val sourceOfSearchMessages =
          sourceOfNumbers.map(num => SearchMessage(s"mrn:$num", s"name:$num", s"dob:$num"))
            .throttle(elements = 100, per = 1 second, maximumBurst = 1, mode = ThrottleMode.Shaping)

        complete(sourceOfSearchMessages)
      }
    }

}
