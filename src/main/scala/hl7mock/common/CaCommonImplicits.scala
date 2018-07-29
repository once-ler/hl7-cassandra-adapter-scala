package com.eztier.hl7mock

import java.util.Date
import ca.uhn.hl7v2.model.Message
import scala.reflect.runtime.universe._
import com.datastax.driver.core.{Row, TypeCodec, UDTValue}
import com.eztier.cassandra.CaCommon.camelToUnderscores
import com.eztier.cassandra.{CaCustomCodecImplicits, CaDefaultUdtCodec, WithInsertStatement}
import com.eztier.hl7mock.types.{CaPatient, CaPatientControl, CaTableDateControl}

object CaCommonImplicits extends CaCustomCodecImplicits {
  override implicit def toCaCodec[T](innerCodec: TypeCodec[UDTValue])(implicit typeTag: TypeTag[T]) = CaDefaultUdtCodec(innerCodec)

  implicit class WrapCaTableDateControl(el: CaTableDateControl) extends WithInsertStatement {
    override def getInsertStatement(keySpace: String) = {
      val insert = el.insertQuery(keySpace)
      insertValues(insert) values(
        camelToUnderscores("Id") -> el.Id,
        camelToUnderscores("CreateDate") -> el.CreateDate
      )
    }
  }

  implicit def rowToCaTableDateControl(row: Row) = {
    CaTableDateControl(
      CreateDate = row.getTimestamp(camelToUnderscores("CreateDate")),
      Id = row.getString(camelToUnderscores("Id"))
    )
  }
}

//
// Typeclass implicits
//

trait CaBase {
  val CreateDate: Date
}

trait CaControl {
  val CreateDate: Date
}

trait MessageTo[T] {
  def decode(m: Message): T
}

object MessageTo {
  import com.eztier.hl7mock.HapiToCaPatientImplicits._

  implicit object MessageToCaPatient extends MessageTo[CaPatient] {
    def decode(m: Message): CaPatient = m
  }
}

trait CaToCaControl[A <: CaBase, B <: CaControl] {
  def decode(a: A): B
}

object CaToCaControl {
  import com.eztier.hl7mock.CaPatientImplicits._

  implicit object CaPatientToCaPatientControl extends CaToCaControl[CaPatient, CaPatientControl] {
    def decode(a: CaPatient): CaPatientControl = a
  }
}

trait CaInsertStatement[A] {
  def encode(a: A): WithInsertStatement
}

object CaInsertStatement {
  import com.eztier.hl7mock.CaPatientImplicits._

  implicit object CaPatientInsertStatement extends CaInsertStatement[CaPatient] {
    def encode(a: CaPatient) = WrapCaPatient(a)
  }

  implicit object CaPatientControlInsertStatement extends CaInsertStatement[CaPatientControl] {
    def encode(a: CaPatientControl) = WrapCaPatientControl(a)
  }
}
