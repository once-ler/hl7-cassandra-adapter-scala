package com.eztier.stream

import akka.stream.scaladsl.Sink
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.CaPatientImplicits
import com.eztier.hl7mock.types._
import com.eztier.stream.CommonTask.balancer

case class HapiToCassandraFlowTask(provider: CaCustomCodecProvider, keySpace: String = "dwh", filter: String = "limit 1") extends WithHapiToCaPatientFlowTrait {
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

  def run(workerCount: Int = 10) = {
    val s = casFlow.getSourceStream(s"select id from dwh.ca_hl_7_control ${filter} allow filtering", 100)

    val f = s
      .via(getLatestHl7Message)
      .via(balancer(transformHl7MessageToCaPatient, workerCount))
      .via(balancer(persist, workerCount))
      .grouped(100000)
      .via(updateDateControl("CaPatientControl"))
      .runWith(Sink.ignore)

    Await.ready(f, Duration.Inf)
  }
}
