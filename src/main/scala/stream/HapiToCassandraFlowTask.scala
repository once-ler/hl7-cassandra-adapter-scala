package com.eztier.stream

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import com.datastax.driver.core.Row

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.types._
import com.eztier.stream.CommonTask.balancer
import com.eztier.stream.traits._
import com.eztier.hl7mock.{CaPatientImplicits, CaToCaControl, CaInsertStatement, MessageTo}

case class HapiToCaPatientFlowTask(provider: CaCustomCodecProvider, keySpace: String = "dwh") extends WithCassandraPersistence {
  // Must register UDT's
  implicit val userImplicits = CaPatientImplicits

  provider.register[CaPatientPhoneInfo]
    .register[CaPatientEmailInfo]
    .register[CaPatientIdType]
    .register[CaPatientNameComponents]
    .register[CaPatientAddress]
    .register[CaPatientCareTeamMember]
    .register[CaPatientEmergencyContact]
    .register[CaPatientEmploymentInformation]

  // alpakka source
  implicit val casFlow = CassandraStreamFlowTask(provider)

  // Decoders
  implicit val caPatientToCaPatientControl = CaToCaControl.CaPatientToCaPatientControl
  implicit val caMessageToCaPatient = MessageTo.MessageToCaPatient

  // Encoders
  implicit val caPatientInsertStatement = CaInsertStatement.CaPatientInsertStatement
  implicit val caPatientControlInsertStatement = CaInsertStatement.CaPatientControlInsertStatement
}

/*
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
      // .toMat(Sink.head)(Keep.right)
      .toMat(sumSink)(Keep.right)
      .run()

    Await.result(f, Duration.Inf)
  }

  def runWithRawStringSource(s: Source[String, NotUsed], workerCount: Int = 10) = {
    val g = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val src = b.add(s)
      val convertHl7ToCaPatient = b.add(Flow[String].map { tryParseHl7Message(_) })

      src ~> convertHl7ToCaPatient

      SourceShape(convertHl7ToCaPatient.out)
    }

    val sr = Source.fromGraph(g)
    persistToCassandra(sr, workerCount)
  }

  def runWithRowSource(s: Source[Row, NotUsed], workerCount: Int = 10) = {
    val g = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val src = b.add(s)
      val getLastestHl7 = b.add(getLatestHl7Message)
      val convertHl7ToCaPatient = b.add(balancer(transformHl7MessageToCaPatient, workerCount))

      src ~> getLastestHl7 ~> convertHl7ToCaPatient

      SourceShape(convertHl7ToCaPatient.out)
    }

    val sr = Source.fromGraph(g)
    persistToCassandra(sr, workerCount)
  }

  def runWithRowFilter(filter: String = "1 = 1 limit 1", workerCount: Int = 10) = {
    val s = casFlow.getSourceStream(s"select id from ${keySpace}.ca_hl_7_control where ${filter} allow filtering", 100)

    runWithRowSource(s, workerCount)
  }
}
*/
