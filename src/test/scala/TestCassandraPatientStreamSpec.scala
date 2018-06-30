package com.eztier.test.cassandra

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.Insert

import java.util.Date
import org.joda.time.DateTime
import org.scalatest.{FunSpec, Matchers}
import scala.concurrent.{Await}
import scala.concurrent.duration._

import com.eztier.cassandra.CaCommon.camelToUnderscores
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.CaPatientImplicits
import com.eztier.hl7mock.types._
import com.eztier.stream.CassandraStreamFlowTask
import com.eztier.stream.CommonTask.{balancer, time}

class TestCassandraPatientStreamSpec extends FunSpec with Matchers {
  // akka
  implicit val system = ActorSystem("Sys")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val logger = system.log

  // hapi singleton
  import com.eztier.hl7mock.Hapi.parseMessage

  // Cassandra CaCustomCodec
  val keySpace = "dwh"

  // Must register UDT's
  implicit val userImplicits = CaPatientImplicits
  import userImplicits._

  import com.eztier.hl7mock.CaCommonImplicits._

  val provider = CaCustomCodecProvider("development.cassandra")

  provider.register[CaPatientPhoneInfo]
    .register[CaPatientEmailInfo]
    .register[CaPatientIdType]
    .register[CaPatientNameComponents]
    .register[CaPatientAddress]
    .register[CaPatientCareTeamMember]
    .register[CaPatientEmergencyContact]
    .register[CaPatientEmploymentInformation]

  // alpakka source
  val casFlow = CassandraStreamFlowTask(provider)

  describe("Cassandra patient suite") {

    it("Should fetch from HL7 control") {
      val f1 = Flow[Seq[Row]].map {
        import com.eztier.hl7mock.CaHl7Implicits._

        rows =>
          rows.map{
            r =>
              val c: CaHl7Control = r
          }
      }

      val f2 = Flow[Row].map {
        a =>
          val id = a.getString("id")
          // This will be the latest message.
          val s = casFlow.getSourceStream(s"select message from dwh.ca_hl_7 where id = '$id' limit 1")
          val f = s.runWith(Sink.head)
          Await.result(f, 30 second)
      }

      val parse = Flow[Row].map {
        import com.eztier.hl7mock.HapiToCaPatientImplicits._

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
        import com.eztier.hl7mock.CaPatientImplicits._

        a =>
          a match {
            case Some(o) =>
              val b: CaPatientControl = o
              writeToDest(o, b)
            case None => new DateTime(1970, 1, 1, 0, 0, 0).toDate
          }
      }

      def updateDateControl(tbl: String) = Flow[Seq[Date]]
        .map{
          a =>
            println(a.length)
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

      val sourceFromDate = "select create_date from dwh.ca_table_date_control where id = 'ca_patient_control' limit 1"
      val s = casFlow.getSourceStream("select id from dwh.ca_hl_7_control where create_date > '2018-05-14 14:00:00' allow filtering", 100)

      time {
        var f = s.via(f2)
          .via(balancer(parse, 10))
          .via(balancer(persist, 10))
          .grouped(100000)
          .via(updateDateControl("CaPatientControl"))
          .runWith(Sink.ignore)


        Await.ready(f, Duration.Inf)
      }

      println("Done")
    }

    it("Should fetch all ids") {
      val s = casFlow.getSourceStream("select id from dwh.ca_hl_7_control", 100)
      var counter = 0
      val f2 = Flow[Row].map {
        a =>
          val id = a.getString("id")
          counter = counter + 1
          logger.error((counter).toString)
      }

      time {
        var f = s.via(f2)
          .runWith(Sink.ignore)

        Await.ready(f, Duration.Inf)
      }
    }

  }
}
