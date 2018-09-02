package com.eztier.hl7mock

import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import com.datastax.driver.core.{ResultSet, SimpleStatement}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import com.eztier.cassandra.CaCommon.getCreateStmt
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.types._

trait CaCreateTypes[A <: CaBase, B <: CaControl] {
  import com.eztier.cassandra.CaCommon.getCreateStmt

  // Create the UDTs and Tables before handing over to the registrar.
  def create(provider: CaCustomCodecProvider)(implicit logger: LoggingAdapter, ec: ExecutionContextExecutor, mat: ActorMaterializer): Either[Exception, Seq[ResultSet]]

  def persist(provider: CaCustomCodecProvider, l: List[String])(implicit logger: LoggingAdapter, ec: ExecutionContextExecutor, mat: ActorMaterializer) = {
    val t = Source(l)
      .mapAsync(1){
        cql => provider.readAsync(new SimpleStatement(cql))
      }
      .log("CaCreateTypes")
      .runWith(Sink.seq)
      .map(Right.apply _)
      .recover{
        case e: Exception => logger.error(e.getMessage)
          Left(e)
      }

    Await.result(t, 30 seconds)
  }

}

object CaCreateTypes {
  // CaTableDateControl Only.
  implicit object CreateCaTableDate extends CaCreateTypes[CaTableDate, CaTableDateControl] {
    override def create(provider: CaCustomCodecProvider)(implicit logger: LoggingAdapter, ec: ExecutionContextExecutor, mat: ActorMaterializer): Either[Exception, Seq[ResultSet]] = {
      val l = {
        getCreateStmt[CaTableDateControl]("Id")("CreateDate")(Some("CreateDate"), Some(-1))
      }.toList

      persist(provider, l)
    }
  }

  // CaHl7
  implicit object CreateCaHl7 extends CaCreateTypes[CaHl7, CaHl7Control] {
    override def create(provider: CaCustomCodecProvider)(implicit logger: LoggingAdapter, ec: ExecutionContextExecutor, mat: ActorMaterializer): Either[Exception, Seq[ResultSet]] = {
      val l = {
        getCreateStmt[CaHl7]("Id")("CreateDate", "MessageType", "ControlId")(Some("CreateDate"), Some(-1)) ++
        getCreateStmt[CaHl7Control]("Id")()(None, None)
      }.toList

      persist(provider, l)
    }
  }

  // CaPatient
  implicit object CreateCaPatient extends CaCreateTypes[CaPatient, CaPatientControl] {

    override def create(provider: CaCustomCodecProvider)(implicit logger: LoggingAdapter, ec: ExecutionContextExecutor, mat: ActorMaterializer): Either[Exception, Seq[ResultSet]] = {
      val l = {
        getCreateStmt[CaPatientPhoneInfo] ++
        getCreateStmt[CaPatientEmailInfo] ++
        getCreateStmt[CaPatientIdType] ++
        getCreateStmt[CaPatientEmergencyContact] ++
        getCreateStmt[CaPatientEmploymentInformation] ++
        getCreateStmt[CaPatientNameComponents] ++
        getCreateStmt[CaPatientAddress] ++
        getCreateStmt[CaPatientCareTeamMember] ++
        getCreateStmt[CaPatient]("Id")("CreateDate")(Some("CreateDate"), Some(-1)) ++
        getCreateStmt[CaPatientControl]("Id")()(None, None)
      }.toList

      persist(provider, l)
    }

  }
}
