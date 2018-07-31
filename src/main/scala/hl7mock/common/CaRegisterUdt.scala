package com.eztier.hl7mock

import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.types._

trait CaRegisterUdt[A <: CaBase, B <: CaControl] {
  def register(provider: CaCustomCodecProvider): CaCustomCodecProvider
}

object CaRegisterUdt {
  // CaPatient
  implicit object RegisterCaPatientUdt extends CaRegisterUdt[CaPatient, CaPatientControl] {
    override def register(provider: CaCustomCodecProvider): CaCustomCodecProvider = {
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
    }
  }
}
