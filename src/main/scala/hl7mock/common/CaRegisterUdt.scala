package com.eztier.hl7mock

import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.types._

trait CaRegisterUdt[A <: CaBase, B <: CaControl] {
  def register(provider: CaCustomCodecProvider): CaCustomCodecProvider
}

object CaRegisterUdt {
  // CaHl7
  implicit object RegisterCaHl7Udt extends CaRegisterUdt[CaHl7, CaHl7Control] {
    import com.eztier.hl7mock.CaHl7Implicits._

    override def register(provider: CaCustomCodecProvider): CaCustomCodecProvider = provider
  }

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
