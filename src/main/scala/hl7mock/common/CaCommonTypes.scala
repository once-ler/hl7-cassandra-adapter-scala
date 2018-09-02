package com.eztier.hl7mock.types

import java.util.Date

import com.eztier.cassandra.CaTbl
import com.eztier.hl7mock.{CaBase, CaControl}

// Note: Not used.
case class CaTableDate(CreateDate: Date = new Date()) extends CaTbl with CaBase

case class CaTableDateControl(
  Id: String = "",
  CreateDate: Date = new Date()
) extends CaTbl with CaControl
