package com.eztier.stream

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import com.datastax.driver.core.Row

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.CaPatientImplicits
import com.eztier.hl7mock.types._
import com.eztier.stream.CommonTask.balancer

case class HapiToCassandraFlowTask(provider: CaCustomCodecProvider, keySpace: String = "dwh") extends WithHapiToCaPatientFlowTrait {
  // Must register UDT's
  implicit val userImplicits = CaPatientImplicits
  import userImplicits._
  import com.eztier.hl7mock.CaCommonImplicits._

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

  def persistToCassandra(s: Source[Option[CaPatient], NotUsed], workerCount: Int = 10) = {
    val f = s
      .via(balancer(persist, workerCount))
      .grouped(100000)
      .via(updateDateControl("CaPatientControl"))
      .toMat(sumSink)(Keep.right)
      .run()

    Await.result(f, Duration.Inf)
  }

  def runWithRawStringSource(s: Source[String, NotUsed], workerCount: Int = 10) = {
    val g = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val f = Flow[String].map { tryParseHl7Message(_) }
      s.via(f).shape
    }

    val sr = Source.fromGraph(g)
    persistToCassandra(sr, workerCount)
  }

  def runWithRowSource(s: Source[Row, NotUsed], workerCount: Int = 10) = {
    val g = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      s
        .via(getLatestHl7Message)
        .via(balancer(transformHl7MessageToCaPatient, workerCount))
        .shape
    }

    val sr = Source.fromGraph(g)
    persistToCassandra(sr, workerCount)
  }

  def runWithRowFilter(filter: String = "limit 1", workerCount: Int = 10) = {
    val s = casFlow.getSourceStream(s"select id from ${keySpace}.ca_hl_7_control ${filter} allow filtering", 100)

    runWithRowSource(s, workerCount)
  }
}
