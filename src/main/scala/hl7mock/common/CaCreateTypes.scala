package com.eztier.hl7mock

import akka.event.LoggingAdapter
import com.datastax.driver.core.SimpleStatement
import scala.concurrent.Await
import scala.concurrent.duration._

import com.eztier.cassandra.CaCommon.getCreateStmt
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.types._

trait CaCreateTypes[A <: CaBase, B <: CaControl] {
  import com.eztier.cassandra.CaCommon.getCreateStmt

  // Create the UDTs and Tables before handing over to the registrar.
  def create(provider: CaCustomCodecProvider)(implicit logger: LoggingAdapter): Either[Exception, Int]
}

object CaCreateTypes {
  // CaPatient
  implicit object CreateCaPatient extends CaCreateTypes[CaPatient, CaPatientControl] {

    override def create(provider: CaCustomCodecProvider)(implicit logger: LoggingAdapter): Either[Exception, Int] = {
      val r = {
        for {
          a <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientPhoneInfo]))
          b <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientEmailInfo]))
          c <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientIdType]))
          d <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientNameComponents]))
          e <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientAddress]))
          f <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientCareTeamMember]))
          g <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientEmergencyContact]))
          h <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientEmploymentInformation]))
          i <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatient]))
          j <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientControl]))
        } yield Right(1)
      }.recover {
        case e: Exception => logger.error(e.getMessage)
        Left(e)
      }

      val s = Await.result(r, 30 seconds)
      s
    }

  }
}
