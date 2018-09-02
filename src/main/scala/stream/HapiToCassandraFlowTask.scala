package com.eztier.stream

import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.stream.traits._
import com.eztier.hl7mock._
import com.eztier.hl7mock.types.{CaTableDate, CaTableDateControl}

case class HapiToCassandraFlowTask[A <: CaBase, B <: CaControl](provider: CaCustomCodecProvider, keySpace: String = "dwh")
  (implicit creator: CaCreateTypes[A, B], registrar: CaRegisterUdt[A, B]) extends WithCassandraPersistence[A, B] {
    // Implicitly create CaTableDateControl if necessary.
    implicitly[CaCreateTypes[CaTableDate, CaTableDateControl]].create(provider)

    // Implicitly create UDT's and tables if necessary.
    creator.create(provider)

    // Implicitly register necessary Cassandra UDT's based on type parameters.
    registrar.register(provider)

    // alpakka source
    implicit val casFlow = CassandraStreamFlowTask(provider)
  }
