package com.eztier.hl7mock

import java.io.{PrintWriter, StringWriter}

import akka.event.LoggingAdapter
import ca.uhn.hl7v2.{DefaultHapiContext, HL7Exception}
import ca.uhn.hl7v2.model.Message
import ca.uhn.hl7v2.parser.{CanonicalModelClassFactory, EncodingNotSupportedException}
import ca.uhn.hl7v2.validation.impl.NoValidation

object Hapi {
  implicit class WrapException[A <: Exception](e: A) {
    def toStackTraceString: String = {
      val sw = new StringWriter
      e.printStackTrace(new PrintWriter(sw))
      sw.toString
    }
  }

  private val pipeParser = {
    val hapiContext = new DefaultHapiContext()
    hapiContext.setModelClassFactory(new CanonicalModelClassFactory("2.3.1"))
    hapiContext.setValidationContext(new NoValidation)
    hapiContext.getPipeParser()
  }

  def parseMessage(in: String)(implicit logger: LoggingAdapter): Option[Message] =
    try {
      val a = pipeParser.parse(in)
      Some(a)
    } catch {
      case e: EncodingNotSupportedException => {
        logger.error(e.toStackTraceString)
        None
      }
      case e1: HL7Exception => {
        logger.error(e1.toStackTraceString)
        None
      }
    }
}
