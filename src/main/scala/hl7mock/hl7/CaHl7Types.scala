package com.eztier.hl7mock.types

import java.util.Date

import com.eztier.cassandra.CaTbl
import com.eztier.hl7mock.{CaBase, CaControl}

case class CaHl7(
  ControlId: String = "",
  CreateDate: Date = new Date(),
  Id: String = "",
  Message: String = "",
  MessageType: String = "",
  Mrn: String = "",
  SendingFacility: String = ""
) extends CaTbl with CaBase

case class CaHl7Control(
  Id: String = "",
  MessageType: String = "",
  CreateDate: Date = new Date()
) extends CaTbl with CaControl
