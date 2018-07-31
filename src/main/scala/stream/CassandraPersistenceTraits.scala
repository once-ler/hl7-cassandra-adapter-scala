package com.eztier.stream.traits

import java.util.Date

import akka.{NotUsed, stream}
import akka.stream.{FlowShape, SinkShape, SourceShape}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Sink, Source}
import com.datastax.driver.core.Row
import com.eztier.hl7mock.{CaBase, CaControl}
import com.eztier.stream.CommonTask.balancer
import com.sun.corba.se.impl.orbutil.graph.Graph

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait WithCassandraPersistence[A <: CaBase, B <: CaControl] extends WithHapiToCassandraFlowTrait[A, B] {
  // Type-class implicits
  import com.eztier.hl7mock._

  def persistToCassandraStage(workerCount: Int = 10)
  (implicit caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B], typeTag: TypeTag[B]) = {
    GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val bcast = b.add(Broadcast[Option[A]](1))
      val save = b.add(balancer(persist, workerCount))
      val gather = b.add(Flow[Date].grouped(100000))
      val controlUpdate = b.add(updateDateControl(typeOf[B].typeSymbol.name.toString))

      bcast ~> save ~> gather ~> controlUpdate

      FlowShape(bcast.in, controlUpdate.out)
    }
  }

  def persistToCassandraFrom(s: Source[Option[A], NotUsed], workerCount: Int = 10)
  (implicit caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B], typeTag: TypeTag[B]) = {
    val persistence = Flow.fromGraph(persistToCassandraStage(workerCount))

    val g = s.via(persistence).toMat(sumSink)(Keep.right)
  }

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
