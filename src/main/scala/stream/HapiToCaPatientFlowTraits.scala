package com.eztier.stream

import java.util.Date
import org.joda.time.DateTime
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream.scaladsl.{Flow, Sink, Source}
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.Insert

import com.eztier.cassandra.CaCommon.camelToUnderscores
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.Hapi.parseMessage
import com.eztier.hl7mock.types.{CaPatient, CaPatientControl, CaTableDateControl}

trait WithHapiToCaPatientFlowTrait {
  import com.eztier.hl7mock.CaCommonImplicits._
  import com.eztier.hl7mock.CaPatientImplicits._
  import com.eztier.hl7mock.HapiToCaPatientImplicits._

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

  val transformHl7MessageToCaPatient = Flow[Row].map {
    a =>
      val msg = a.getString("message")
      val m = parseMessage(msg)
      m match {
        case Some(a) =>
          val c: CaPatient = a
          Some(c)
        case _ => None
      }
  }

  def writeToDest(a: (CaPatient, CaPatientControl)) = {
    val ins1 = a._1 getInsertStatement(keySpace)
    val ins2 = a._2 getInsertStatement(keySpace)

    val f = Source[Insert](List(ins1, ins2))
      .via(provider.getInsertFlow())
      .runWith(Sink.ignore)

    Await.ready(f, 30 second)
    a._1.CreateDate
  }

  val persist = Flow[Option[CaPatient]].map {
    a =>
      a match {
        case Some(o) =>
          val b: CaPatientControl = o
          writeToDest(o, b)
        case None => new DateTime(1970, 1, 1, 0, 0, 0).toDate
      }
  }

  def updateDateControl(tbl: String) = Flow[Seq[Date]]
    .map {
      a =>
        val uts = a.max
        val c3 = CaTableDateControl(
          Id = camelToUnderscores(tbl),
          CreateDate = uts
        )
        val ins3 = c3 getInsertStatement(keySpace)

        val f = Source[Insert](List(ins3))
          .via(provider.getInsertFlow())
          .runWith(Sink.ignore)

        Await.ready(f, 30 second)
    }
}
