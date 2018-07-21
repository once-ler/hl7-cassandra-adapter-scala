import akka.stream.scaladsl.Source
import com.eztier.cassandra.CaCustomCodecProvider
import com.eztier.stream.HapiToCassandraFlowTask
import org.scalatest.{FunSpec, Matchers}

class TestHapiToCassandraStreamSpec extends FunSpec with Matchers {
  describe("Hapi to Cassandra Patient Suite") {
    val provider = CaCustomCodecProvider("development.cassandra")

    val flow = HapiToCassandraFlowTask(provider = provider, keySpace = "dwh")

    it("Fetch a stream of Hl7 messages with a filter, transform them to CaPatient, and perist them in Cassandra") {
      val res = flow.runWithRowFilter("create_date > '2018-05-14 14:00:00'", 10)
    }

    it("Fetch a stream of Hl7 messages from an akka Source, transform them to CaPatient, and perist them in Cassandra") {
      val s = flow.casFlow.getSourceStream("select id from dwh.ca_hl_7_control where create_date > '2018-05-14 14:00:00' allow filtering", 100)

      val res = flow.runWithRowSource(s, 10)
    }

    it("From a iterable of raw Hl7 messages as a akka Source, transform them to CaPatient, and perist them in Cassandra") {
      val msgs = List("", "", "")
      val s = Source(msgs)

      val res = flow.runWithRawStringSource(s, 10)
    }

  }
}
