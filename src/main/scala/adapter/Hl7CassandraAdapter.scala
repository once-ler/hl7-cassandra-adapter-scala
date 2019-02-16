package com.eztier.adapter

import java.io.{PrintWriter, StringWriter}

import akka.actor.ActorSystem
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.hl7mock.{CaBase, CaControl, CaCreateTypes, CaRegisterUdt}
import com.eztier.stream.HapiToCassandraFlowTask
import com.typesafe.config.Config

class Hl7CassandraAdapter[A <: CaBase, B <: CaControl](provider: CaCustomCodecProvider, keySpace: String)
  (implicit creator: CaCreateTypes[A, B], registrar: CaRegisterUdt[A, B]){

    val flow = HapiToCassandraFlowTask[A, B](provider, keySpace)

}

object Hl7CassandraAdapter {
  private implicit val system = ActorSystem("Sys")
  private implicit val logger = system.log

  def apply[A <: CaBase, B <: CaControl](configPath: String, keySpace: String, conf: Option[Config] = None)(implicit creator: CaCreateTypes[A, B], registrar: CaRegisterUdt[A, B]) = {
    val maybeProvider: Either[String, CaCustomCodecProvider] = try {
      conf match {
        case Some(c) => Right(CaCustomCodecProvider(c)(configPath))
        case _ => Right(CaCustomCodecProvider(configPath))
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        Left(e.getMessage.concat(sw.toString))
    }

    val provider = maybeProvider match {
      case Right(x) => x
      case Left(x) =>
        logger.error(x)
        throw new Exception(x)
    }

    new Hl7CassandraAdapter[A, B](provider, keySpace)
  }
}
