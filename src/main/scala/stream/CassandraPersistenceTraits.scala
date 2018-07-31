package com.eztier.stream.traits

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Source}
import com.datastax.driver.core.Row
import com.eztier.hl7mock.{CaBase, CaControl}
import com.eztier.stream.CommonTask.balancer

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait WithCassandraPersistence[A <: CaBase, B <: CaControl] extends WithHapiToCassandraFlowTrait[A, B] {
  // Type-class implicits
  import com.eztier.hl7mock._

  def persistToCassandra(s: Source[Option[A], NotUsed], workerCount: Int = 10)
  (implicit caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B], typeTag: TypeTag[B]) = {
    val f = s
      .via(balancer(persist, workerCount))
      .grouped(100000)
      .via(updateDateControl(typeOf[B].typeSymbol.name.toString))
      .toMat(sumSink)(Keep.right)
      .run()

    Await.result(f, Duration.Inf)
  }

  def runWithRawStringSource(s: Source[String, NotUsed], workerCount: Int = 10)
  (implicit messageToConverter: MessageTo[A], caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B], typeTag: TypeTag[B]) = {
    val g = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val src = b.add(s)
      val convertHl7ToCa = b.add(Flow[String].map { tryParseHl7Message(_) })

      src ~> convertHl7ToCa

      SourceShape(convertHl7ToCa.out)
    }

    val sr = Source.fromGraph(g)
    persistToCassandra(sr, workerCount)
  }

  def runWithRowSource(s: Source[Row, NotUsed], workerCount: Int = 10)
  (implicit messageToConverter: MessageTo[A], caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B], typeTag: TypeTag[B]) = {
    val g = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val src = b.add(s)
      val getLastestHl7 = b.add(getLatestHl7Message)
      val convertHl7ToCaType = b.add(balancer(transformHl7MessageToCaType, workerCount))

      src ~> getLastestHl7 ~> convertHl7ToCaType

      SourceShape(convertHl7ToCaType.out)
    }

    val sr = Source.fromGraph(g)
    persistToCassandra(sr, workerCount)
  }

  def runWithRowFilter(filter: String = "", workerCount: Int = 10)(
  implicit
    messageToConverter: MessageTo[A],
    caToCaControlConverter: CaToCaControl[A, B],
    baseConverter: CaInsertStatement[A],
    controlConverter: CaInsertStatement[B],
    typeTag: TypeTag[B]
  ) = {
    val prefix = s"select id from ${keySpace}.ca_hl_7_control"
    val stmt = if (filter.length > 0) s"${prefix} where ${filter} allow filtering" else s"${prefix} limit 1"

    val s = casFlow.getSourceStream(stmt, 100)
    runWithRowSource(s, workerCount)
  }
}
