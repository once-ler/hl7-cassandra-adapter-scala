package com.eztier.test.cassandra

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.datastax.driver.core._
import org.scalatest.{Matchers, fixture}

import scala.concurrent.{Await}
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.cassandra.CaCommon.{camelToUnderscores}
import com.eztier.hl7mock.CaPatientImplicits
import com.eztier.hl7mock.types._

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

          // preparedStatement provided by user.
          implicit val session = provider.session
          val preparedStatement = getPreparedStatement("dwh", el)

          // statementBinder provided by user.
          val boundStatement = getStatementBinder(el, preparedStatement)

          import scala.concurrent.duration.Duration
          val fut0 = provider.insertAsync(boundStatement)
          val res = Await.result(fut0, Duration.Inf)

          println("Binder")

          val stmt1 = new SimpleStatement(s"select * from dwh.${camelToUnderscores(el.getClass.getSimpleName)}").setFetchSize(20)

          val fut = provider.readAsync(stmt1)
          val rs = Await.result(fut, Duration.Inf)

          val row: Row = rs.one()

          val addresses = row.getList("addresses", classOf[CaPatientAddress])

          val id = row.getString("id")

          val dt = row.getTimestamp("create_date")

          val patient: CaPatient = row

          println("Simple read")

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

          println("Done")
      }
  }
}
