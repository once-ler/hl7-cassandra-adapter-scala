package com.eztier.test.cassandra

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import com.datastax.driver.core._
import com.eztier.adapter.Hl7CassandraAdapter
import org.scalatest.{Matchers, fixture}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.cassandra.CaCommon.camelToUnderscores
import com.eztier.hl7mock.{CaBase, CaControl, CaCreateTypes, CaPatientImplicits}
import com.eztier.hl7mock.types._
import com.typesafe.config.ConfigFactory

class TestHl7CassandraAdapterSpec extends fixture.FunSpec with Matchers with fixture.ConfigMapFixture {

  implicit val system = ActorSystem("Sys")
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val logger = system.log

  def withSearchCriteria(test: Option[String] => Any): Unit = {
    test(Some("EBF771B0B8A10A43A39C3CE8EF4D7F18"))
  }

  it("Can create a UDT codec on the fly") {
    () =>
      withSearchCriteria {
        cri =>

          // User defined implicits.
          implicit val userImplicits = CaPatientImplicits

          import userImplicits._

          val provider = CaCustomCodecProvider("development.cassandra")

          provider.register[CaPatientPhoneInfo]
            .register[CaPatientEmailInfo]
            .register[CaPatientIdType]
            .register[CaPatientNameComponents]
            .register[CaPatientAddress]
            .register[CaPatientCareTeamMember]
            .register[CaPatientEmergencyContact]
            .register[CaPatientEmploymentInformation]

          println("Registered")

          val el = CaPatient(Id = "12345678")

          val insertStatement = el getInsertStatement("dwh")
          val qs = insertStatement.getQueryString()
          val stmt0 = new SimpleStatement(insertStatement.toString)

          println("Insert Statement")

          /*
          // TODO: Need to allow user to provide the bounded fields instead of automatically generating it in lexiconal order.
          // preparedStatement provided by user.
          implicit val session = provider.session
          val preparedStatement = getPreparedStatement("dwh", el)

          // statementBinder provided by user.
          val boundStatement = getStatementBinder(el, preparedStatement)

          import scala.concurrent.duration.Duration
          val fut0 = provider.insertAsync(boundStatement)
          val res = Await.result(fut0, Duration.Inf)

          println("Binder")

          */
          val stmt1 = new SimpleStatement(s"select * from dwh.${camelToUnderscores(el.getClass.getSimpleName)}").setFetchSize(20)

          val fut = provider.readAsync(stmt1)
          val rs = Await.result(fut, 10 seconds)

          val row: Row = rs.one()

          val addresses = row.getList("addresses", classOf[CaPatientAddress])

          val id = row.getString("id")

          val dt = row.getTimestamp("create_date")

          val patient: CaPatient = row

          patient shouldBe a [CaPatient]

      }
  }

  it("Should create insert statement for common table") {

    import com.eztier.hl7mock.CaCommonImplicits._

    () =>
      withSearchCriteria {
        cri =>
          val x = CaHl7Control().getClass.getSimpleName
          val el = CaTableDateControl(Id = x)
          val insertStatement = el.getInsertStatement("dwh")
          val qs = insertStatement.getQueryString()
          val stmt0 = new SimpleStatement(insertStatement.toString)

          stmt0.toString should startWith ("INSERT INTO dwh.ca_table_date_control (id,create_date) VALUES ('CaHl7Control',")
      }
  }

  it("Should create UDT's and tables if necessary") {

    () =>
      import com.eztier.hl7mock.CaCreateTypes._

      object CreatorUtil {
        def create[A <: CaBase, B <: CaControl](provider: CaCustomCodecProvider)(implicit creator: CaCreateTypes[A, B]) = {
          creator.create(provider)
        }
      }

      withSearchCriteria {
        cri =>
          // Make sure the keyspace defined is actually created in cassandra.
          // create keyspace if not exists dwh with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
          val provider = CaCustomCodecProvider("development.cassandra")

          val either = CreatorUtil.create[CaHl7, CaHl7Control](provider)

          either should be ('right)

          val either1 = CreatorUtil.create[CaPatient, CaPatientControl](provider)

          // Expect Right(ResultSet[ exhausted: true, Columns[]])
          either1 should be ('right)
      }
  }

  it("Should create adapter and can process Hl7 messages and persist to Cassandra") {
    () =>
      withSearchCriteria {
        cri =>

          val adapter = Hl7CassandraAdapter[CaPatient, CaPatientControl]("development.cassandra", "dwh")

          val res = adapter.flow.runWithRowFilter("create_date > '1998-07-26 15:00:00' limit 10", 10)

          res should be > 0
      }
  }

  it("Should parse a raw HL7 string and return a cassandra persistence future") {
    () =>
      withSearchCriteria {
        cri =>
          val fixtures = ConfigFactory.load("fixtures")
          val msg = fixtures.getStringList("spec-test.mdm-t02").toArray.mkString("")

          val s = Source.single(msg)

          val adapter = Hl7CassandraAdapter[CaPatient, CaPatientControl]("development.cassandra", "dwh")

          // 1 worker count
          val res = adapter.flow.runWithRawStringSource(s, 1)

          res shouldBe a [Future[_]]

          val count = Await.result(res, 5 seconds)

          count should be (1)
      }
  }

}
