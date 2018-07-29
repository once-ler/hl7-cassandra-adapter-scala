package com.eztier.stream.traits

import akka.NotUsed
import akka.stream.SourceShape
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Source}
import com.datastax.driver.core.Row
import com.eztier.stream.CommonTask.balancer

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait WithCassandraPersistence extends WithHapiToCassandraFlowTrait {
  // Type-class implicits
  import com.eztier.hl7mock._

  def persistToCassandra[A <: CaBase, B <: CaControl](s: Source[Option[A], NotUsed], workerCount: Int = 10)
  (implicit caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B], typeTag: TypeTag[B]) = {
    val f = s
      .via(balancer(persist[A, B], workerCount))
      .grouped(100000)
      .via(updateDateControl(typeOf[B].typeSymbol.name.toString))
      .toMat(sumSink)(Keep.right)
      .run()

    Await.result(f, Duration.Inf)
  }

  def runWithRawStringSource[A <: CaBase, B <: CaControl](s: Source[String, NotUsed], workerCount: Int = 10)
  (implicit messageToConverter: MessageTo[A], caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B], typeTag: TypeTag[B]) = {
    val g = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val src = b.add(s)
      val convertHl7ToCa = b.add(Flow[String].map { tryParseHl7Message[A](_) })

      src ~> convertHl7ToCa

      SourceShape(convertHl7ToCa.out)
    }

    val sr = Source.fromGraph(g)
    persistToCassandra[A, B](sr, workerCount)
  }

  def runWithRowSource[A <: CaBase, B <: CaControl](s: Source[Row, NotUsed], workerCount: Int = 10)
  (implicit messageToConverter: MessageTo[A], caToCaControlConverter: CaToCaControl[A, B], baseConverter: CaInsertStatement[A], controlConverter: CaInsertStatement[B], typeTag: TypeTag[B]) = {
    val g = GraphDSL.create() { implicit b =>
      import GraphDSL.Implicits._

      val src = b.add(s)
      val getLastestHl7 = b.add(getLatestHl7Message)
      val convertHl7ToCaType = b.add(balancer(transformHl7MessageToCaType[A], workerCount))

      src ~> getLastestHl7 ~> convertHl7ToCaType

      SourceShape(convertHl7ToCaType.out)
    }

    val sr = Source.fromGraph(g)
    persistToCassandra[A, B](sr, workerCount)
  }

  def runWithRowFilter[A <: CaBase, B <: CaControl](filter: String = "", workerCount: Int = 10)(
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
    runWithRowSource[A, B](s, workerCount)
  }
}
