package com.eztier.hl7mock

import akka.stream.scaladsl.{Flow, Source}
import com.datastax.driver.core.SimpleStatement
import com.eztier.cassandra.CaCommon.getCreateStmt
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.types._

trait CaCreateTypes[A <: CaBase, B <: CaControl] {
  import com.eztier.cassandra.CaCommon._

  // Create the UDTs and Tables before handing over to the registrar.
  def create(provider: CaCustomCodecProvider): CaCustomCodecProvider
}

object CaCreateTypes {
  // CaPatient
  implicit object CreateCaPatient extends CaCreateTypes[CaPatient, CaPatientControl] {
    override def create(provider: CaCustomCodecProvider): CaCustomCodecProvider = {

      Source.single(1)
        .via(Flow[Int].mapAsync(1){_ => provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientPhoneInfo])) })

      val r = for {
        a <- provider.readAsync(new SimpleStatement(getCreateStmt[CaPatientPhoneInfo]))
        b <- provider.session.executeAsync(getCreateStmt[CaPatientEmailInfo])
        c <- provider.session.executeAsync(getCreateStmt[CaPatientIdType])
        d <- provider.session.executeAsync(getCreateStmt[CaPatientNameComponents])
        e <- provider.session.executeAsync(getCreateStmt[CaPatientAddress])
        f <- provider.session.executeAsync(getCreateStmt[CaPatientCareTeamMember])
        g <- provider.session.executeAsync(getCreateStmt[CaPatientEmergencyContact])
        h <- provider.session.executeAsync(getCreateStmt[CaPatientEmploymentInformation])
        i <- provider.session.executeAsync(getCreateStmt[CaPatient])
        j <- provider.session.executeAsync(getCreateStmt[CaPatientControl])
      } yield 1



    }
  }
}
