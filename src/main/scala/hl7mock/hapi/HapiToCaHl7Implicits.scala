package com.eztier.hl7mock

import java.text.SimpleDateFormat

import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.util.Terser
import com.eztier.hl7mock.types.{CaHl7, CaHl7Control}

object HapiToCaHl7Implicits {
  implicit def fromMessageToCaHl7(in: Message): CaHl7 = {
    val terser = new Terser(in)

    val fa = terser.get("/MSH-4")
    val ev = terser.get("/.MSH-9-1")
    val ty = terser.get("/MSH-9-2")
    val ts = terser.get("/MSH-7")
    val pid = try { terser.get("/PID-2") } catch {case e: Exception => "UNKNOWN"}
    val ct = terser.get("/MSH-10")
    val fmt = ts.length match {
      case 14 => "yyyyMMddHHmmss"
      case 12 => "yyyyMMddHHmm"
      case 10 => "yyyyMMddHH"
      case _ => "yyyyMMdd"
    }
    val uts = new SimpleDateFormat(fmt).parse(ts)

    CaHl7(
      Id = pid,
      Mrn = pid,
      SendingFacility = if (fa == null) "" else fa,
      CreateDate = uts,
      Message = in.encode,
      MessageType = ev + "^" + ty,
      ControlId = if (ct == null) "" else ct
    )
  }
}
