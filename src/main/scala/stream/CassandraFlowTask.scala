package com.eztier.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.cassandra.scaladsl.{CassandraSource}
import com.datastax.driver.core._
import com.eztier.cassandra.CaCustomCodecProvider

case class CassandraStreamFlowTask(endpoint: String = "127.0.0.1", user: String = "", pass: String = "", port: Int = 9042) {
  implicit lazy val session = Cluster.builder
    .addContactPoint(endpoint)
    .withPort(port)
    .withCredentials(user, pass)
    .build
    .connect()

  implicit val system = ActorSystem()
  implicit val ec = system.dispatcher
  implicit val mat = ActorMaterializer()

  def getSourceStream(cqlStmt: String, fetchSize: Int = 20) = {
    val stmt = new SimpleStatement(cqlStmt).setFetchSize(fetchSize)
    CassandraSource(stmt)
  }
}

/*
@Usage:
  val provider = val provider = CaCustomCodecProvider("casprod")
  // Register Custom Codecs
  // provider.register[...]

  val cf = CassandraStreamFlowTask(provider)
 */
object CassandraStreamFlowTask {
  def apply(caCustomCodecProvider: CaCustomCodecProvider) = {
    new CassandraStreamFlowTask() {
      implicit override lazy val session = caCustomCodecProvider.session
    }
  }
}
