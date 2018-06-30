package com.eztier.test.hl7

import java.text.SimpleDateFormat
import java.util.Date
import org.joda.time.DateTime
import org.scalatest.{FunSpec, Matchers}

import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.{DefaultHapiContext, HL7Exception}
import ca.uhn.hl7v2.model.v231.datatype.{CE, TS, XPN}
import ca.uhn.hl7v2.model.v231.segment.{NK1, PID}
import ca.uhn.hl7v2.parser.{CanonicalModelClassFactory, EncodingNotSupportedException}
import ca.uhn.hl7v2.util.Terser
import ca.uhn.hl7v2.validation.impl.NoValidation

import com.eztier.hl7mock.types.{CaPatientEmergencyContact, CaPatientIdType, CaPatientNameComponents, CaPatientPhoneInfo}

class TestHL7Spec extends FunSpec with Matchers {

  val msg = "MSH|^~\\&|HIS|RIH|EKG|EKG|199904140038||ADT^A01||P|2.3\r" +
    "PID|0001|00009874|00001122|A00977|SMITH^JOHN^M|MOM|19581119|F|NOTREAL^LINDA^M|C|564 SPRING ST^^NEEDHAM^MA^02494^US|0002|(818)565-1551|(425)828-3344|E|S|C|0000444444|252-00-4414||||SA|||SA||||NONE|V1|0001|I|D.ER^50A^M110^01|ER|P00055|11B^M011^02|070615^BATMAN^GEORGE^L|555888^NOTREAL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^NOTREAL^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|199904101200||||5555112333|||666097^NOTREAL^MANNY^P\r" +
    "PD1|||CHILDREN=S HOSPITAL^^1234^^^^XX~LEXINGTON CLINIC^^1234A^^^^FI|12345^CARE^ PRIMARY^^^DR^MD^^^L^^^DN|||||||03^REMINDER/RECALL - NO CALLS^HL70215|Y\r" +
    "NK1|1|JONES^BARBARA^K|WIFE||||||NK^NEXT OF KIN||||||||||\r" +
    "PV1|0001|I|D.ER^1F^M950^01|ER|P000998|11B^M011^02|070615^BATMAN^GEORGE^L|555888^OKNEL^BOB^K^DR^MD|777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^VOICE^BILL^L|ER|000001916994|D||||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|||||5555112333|||666097^DNOTREAL^MANNY^P\r" +
    "PV2|||0112^TESTING|55555^PATIENT IS NORMAL|NONE|||19990225|19990226|1|1|TESTING|555888^NOTREAL^BOB^K^DR^MD||||||||||PROD^003^099|02|ER||NONE|19990225|19990223|19990316|NONE\r" +
    "AL1||SEV|001^POLLEN\r" +
    "GT1||0222PL|NOTREAL^BOB^B||STREET^OTHER STREET^CITY^ST^77787|(444)999-3333|(222)777-5555||||MO|111-33-5555||||NOTREAL GILL N|STREET^OTHER STREET^CITY^ST^99999|(111)222-3333\r" +
    "IN1||022254P|4558PD|BLUE CROSS|STREET^OTHER STREET^CITY^ST^00990||(333)333-6666||221K|LENIX|||19980515|19990515|||PATIENT01 TEST D||||||||||||||||||02LL|022LP554"

  val msg3 = "MSH|^~\\&|Cerner|Cerner|ALLSCRIPT|ALLSCRIPT|20090206103539||MDM^T02|718387|P|2.4||||||||\r" +
    "EVN|T02|20090206103539|||WKF||\r" +
    "PID|1001|00009874^^^^^|6666666^^^^^|77777777^^^^^|SMITH^JOHN^^^^|^^^^^|19531205|F||||||||||703090710|059263323||||||||||||\r" +
    "PV1|||^^^^^^^^||^^^^^|^^^^^^^^|1510000000^JONES^FRED^^^^^^^^^^^|^^^^^^^^^^^^^|^^^^^^^^^^^^^|ALLSCRIPT|||||||^^^^^^^^^^^^^|OUTPATIENT|^^^^^||||||||||||||||||||||||||||||||||\r" + "" +
    "TXA||Universal Consent for Biospecimen||20090206103539|^^^||||^|||^^155949097^|^^^|^^^|^^^||AU|U|AV|I||^20090206103539||\r" +
    "OBX|1|RP|URL^URL^||155949097^^^|||||||||||||\r" +
    "NTE|||CONSENT GIVEN|\r" +
    "OBX|2|ST|Blood and Tissue Consent^||Y^^^|||||||||20090206103418||||\r" +
    "OBX|3|ST|Additional Blood Consent^||Y^^^|||||||||20090206103418||||\r" +
    "OBX|4|ST|Other Consent^||Y^^^|||||||||20090206103418||||"

  describe("HL7 Suite") {

    val hapiContext = new DefaultHapiContext()
    hapiContext.setModelClassFactory(new CanonicalModelClassFactory("2.3.1"))
    hapiContext.setValidationContext(new NoValidation)
    val p = hapiContext.getPipeParser()

    val hpiMsgMaybe: Option[Message] = {
      try {
        val hpiMsg = p.parse(msg3)
        Some(hpiMsg)
      } catch {
        case e: EncodingNotSupportedException => { e.printStackTrace(); None }
        case e1: HL7Exception => { e1.printStackTrace(); None }
      }
    }

    val terserMaybe: Option[Terser] = {
      hpiMsgMaybe match {
        case Some(a) => Some(new Terser(a))
        case _ => None
      }
    }

    val terser = terserMaybe.get

    it("Should be able to use terser") {
      val fa = terser.get("/MSH-4")
      val ev = terser.get("/.MSH-9-1")
      val ty = terser.get("/MSH-9-2")
      val ts = terser.get("/MSH-7")
      val pid = terser.get("/PID-2")
      val ct = terser.get("/MSH-10")

      // 2016-12-07-15:01:35
      // val unixMs = ts.getTimeOfAnEvent.getValueAsDate.getTime

      val fmt = new SimpleDateFormat("yyyyMMddHHmmss")
      val uts = fmt.parse(ts).getTime
      val dt = new Date(uts)

      fa should be ("Cerner")
      ev should be ("MDM")
      ty should be ("T02")
      pid should be ("00009874")
      ct should be ("718387")

      dt should be ((new DateTime(2009, 2, 6, 10, 35, 39)).toDate)
    }

    it("Should parse MDM PID structure") {
      val hpiMsg = hpiMsgMaybe.get
      val originalMsg = hpiMsg.encode()
      val pid3 = hpiMsg.get("PID").asInstanceOf[PID]
      val a = pid3.getPatientIdentifierList.head.getID
    }

    it("Should parse PID") {
      val hpiMsg = hpiMsgMaybe.get

      val pid = hpiMsg.get("PID").asInstanceOf[PID]

      val gender = pid.getSex.getValueOrEmpty

      gender should be ("F")

      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val dob = pid.getDateTimeOfBirth.getTimeOfAnEvent.getValueAsDate
      val dobStr = sdf.format(dob)

      dobStr should be ("1953-12-05")

      val ids = pid.getPatientIdentifierList.map {
        a =>
          CaPatientIdType(
            Id = a.getID.getValueOrEmpty,
            Type = a.getIdentifierTypeCode.getValueOrEmpty
          )
      }

      val altIds = pid.getAlternatePatientIDPID.map {
        a =>
          a.getID.getValueOrEmpty
      }

      val alternateId = altIds.headOption.getOrElse("")

      alternateId should be ("77777777")

      val mrn = ids.headOption.getOrElse(CaPatientIdType()).Id

      mrn should be ("6666666")

      val nameComponents = pid.getPatientName.map {
        a =>
          CaPatientNameComponents(
            FirstName = a.getGivenName.getValueOrEmpty,
            MiddleName = a.getMiddleInitialOrName.getValueOrEmpty,
            LastName = a.getFamilyLastName.getFamilyName.getValueOrEmpty,
            LastNamePrefix = a.getFamilyLastName.getLastNamePrefix.getValueOrEmpty,
            Academic = a.getDegreeEgMD.getValueOrEmpty,
            Suffix = a.getSuffixEgJRorIII.getValueOrEmpty,
            Title = a.getPrefixEgDR.getValueOrEmpty
          )
      }

      val nameF = nameComponents.headOption.getOrElse(CaPatientNameComponents())

      val name = s"${nameF.LastName}, ${nameF.FirstName}${if (nameF.MiddleName.length > 0) " " + nameF.MiddleName else ""}"

      name should be ("SMITH, JOHN")
    }

    it("Should parse NK1") {
      val hpiMsg2 = p.parse(msg)

      val nk1 = hpiMsg2.get("NK1").asInstanceOf[NK1]

      nk1 shouldBe a [NK1]

      val nk1All = hpiMsg2.getAll("NK1")

      val c = nk1All.map {
        o =>
          val a = o.asInstanceOf[NK1]

          val name = a.getNKName.headOption.getOrElse(new XPN(a.getMessage))
          val mi = name.getMiddleInitialOrName.getValueOrEmpty
          CaPatientEmergencyContact(
            LegalGuardian = "",
            Name = name.getFamilyLastName.getFamilyName.getValueOrEmpty + ", " + name.getGivenName.getValueOrEmpty + { if (mi.length > 0) " " + mi else "" },
            PhoneNumbers = Seq[CaPatientPhoneInfo](),
            Relation = a.getRelationship.getIdentifier.getValueOrEmpty
          )
      }

      val contact0 = c(0)
      contact0 shouldBe a [CaPatientEmergencyContact]

      contact0.Name should be ("JONES, BARBARA K")
      contact0.Relation should be ("WIFE")

    }
  }
}
