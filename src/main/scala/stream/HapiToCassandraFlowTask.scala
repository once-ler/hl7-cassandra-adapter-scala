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
}
