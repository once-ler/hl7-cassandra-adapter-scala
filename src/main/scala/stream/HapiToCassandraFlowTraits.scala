package com.eztier.stream.traits

import java.util.Date

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.Insert
import com.eztier.cassandra.CaCommon.camelToUnderscores
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.{CaBase, CaControl}
import com.eztier.hl7mock.Hapi.parseMessage
import com.eztier.hl7mock.types._
import com.eztier.stream.CassandraStreamFlowTask
import org.joda.time.DateTime

trait WithHapiToCassandraFlowTrait[A <: CaBase, B <: CaControl] {
  // For CaTableDateControl
  import com.eztier.hl7mock.CaCommonImplicits._
  // Type-class implicits
  import com.eztier.hl7mock._

  // akka
  implicit val system = ActorSystem("Sys")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val logger = system.log

  implicit val provider: CaCustomCodecProvider
  implicit val casFlow: CassandraStreamFlowTask
  implicit val keySpace: String

  val getLatestHl7Message = Flow[Row].map {
    a =>
      val id = a.getString("id")
      // This will be the latest message.
      val s = casFlow.getSourceStream(s"select message from dwh.ca_hl_7 where id = '$id' limit 1")
      val f = s.runWith(Sink.head)
      Await.result(f, 30 second)
  }

  def tryParseHl7Message(msg: String)(implicit converter: MessageTo[A]): Option[A] = {
    val m = parseMessage(msg)
    m match {
      case Some(a) =>
        val c: A = converter.decode(a)
        Some(c)
      case _ => None
    }
  }

  def transformHl7MessageToCaType(implicit messageToConverter: MessageTo[A]) = Flow[Row].map {
    a =>
      val msg = a.getString("message")
      tryParseHl7Message(msg)
  }

  def writeToDest(a: (A, B))
  (implicit caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B]) = {
    val a1 = baseConverter.encode(a._1)
    val a2 = controlConverter.encode(a._2)

    val ins1 = a1 getInsertStatement(keySpace)
    val ins2 = a2 getInsertStatement(keySpace)
    val f = Source[Insert](List(ins1, ins2))
      .via(provider.getInsertFlow())
      .runWith(Sink.ignore)

    Await.ready(f, 120 second)
    a._1.CreateDate
  }

  def persist
  (implicit caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B]) = Flow[Option[A]].map {
      a =>
        a match {
          case Some(o) =>
            val b: B = caToCaControlConverter.decode(o)
            writeToDest(o, b)
          case None => new DateTime(1970, 1, 1, 0, 0, 0).toDate
        }
    }

  def sumSink: Sink[Int, Future[Int]] = Sink.fold[Int, Int](0)(_ + _)

  def updateDateControl(tbl: String) = Flow[Seq[Date]]
    .mapAsync(1) {
      a =>
        val uts = a.max
        val c3 = CaTableDateControl(
          Id = camelToUnderscores(tbl),
          CreateDate = uts
        )
        val ins3 = c3 getInsertStatement(keySpace)

        Source[Insert](List(ins3))
          .via(provider.getInsertFlow())
          .map(_ => a.length)
          .toMat(sumSink)(Keep.right)
          .run()
    }
}
