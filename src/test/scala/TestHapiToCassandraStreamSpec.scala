package com.eztier.test.cassandra

import java.io.{PrintWriter, StringWriter}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.types.{CaHl7, CaHl7Control, CaPatient, CaPatientControl}
import com.eztier.stream._
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class TestHapiToCassandraStreamSpec extends FunSpec with Matchers {
  implicit val system = ActorSystem("Sys")
  implicit val ec = system.dispatcher
  implicit val logger = system.log

  describe("Hapi to Cassandra Patient Suite") {
    val maybeProvider: Either[String, CaCustomCodecProvider] = try {
      Right(CaCustomCodecProvider("development.cassandra"))
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        Left(e.getMessage.concat(sw.toString))
    }

    val provider = maybeProvider match {
      case Right(x) => x
      case Left(x) =>
        logger.error(x)
        throw new Exception(x)
    }

    it("Should parse raw hl7 message and persist to cassandra hl7 type") {
      val fixtures = ConfigFactory.load("fixtures")
      val adtMsg = fixtures.getStringList("spec-test.another-adt-a01").toArray.mkString("")

      val flow = HapiToCassandraFlowTask[CaHl7, CaHl7Control](provider = provider, keySpace = "dwh")
      val s = Source.single(adtMsg)
      val f = flow.runWithRawStringSource(s, 10)

      val res = Await.result(f, 10 seconds)

      res should be (1)
    }

    it("Fetch a stream of cassandra hl7 type with a filter, transform them to CaPatient, and perist them in Cassandra") {
      val flow = HapiToCassandraFlowTask[CaPatient, CaPatientControl](provider = provider, keySpace = "dwh")
      val res = flow.runWithRowFilter("create_date > '1998-07-26 15:00:00' limit 1", 10)

      res should be (1)
    }

    it("Fetch a stream of cassandra hl7 type from an akka Source, transform them to CaPatient, and perist them in Cassandra") {
      val flow = HapiToCassandraFlowTask[CaPatient, CaPatientControl](provider = provider, keySpace = "dwh")
      val s = flow.casFlow.getSourceStream(s"select id from dwh.ca_hl_7_control where create_date > '1998-05-14 14:00:00' limit 1 allow filtering", 100)

      val res = flow.runWithRowSource(s, 10)

      res should be (1)
    }

  }
}
