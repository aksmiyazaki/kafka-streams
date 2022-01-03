import sbt._

object Dependencies {
  lazy val V = new {
    val AVRO = "1.8.2"
    val KAFKA = "1.0.0"
    val KAFKA_STREAMS = "3.0.0"
    val LOG4J = "1.7.32"
    val CONFLUENT = "4.0.0"
    val SCALATEST = "3.2.10"
  }

  lazy val buildDependencies = Seq(
    "org.apache.avro" % "avro" % V.AVRO,
    "org.apache.kafka" % "kafka-streams" % V.KAFKA_STREAMS,
    "org.slf4j" % "slf4j-api" % V.LOG4J,
    "org.slf4j" % "slf4j-log4j12" % V.LOG4J,
    "org.apache.kafka" % "kafka-clients" % V.KAFKA,
    "io.confluent" % "kafka-avro-serializer"  % V.CONFLUENT,
    "io.confluent" % "kafka-streams-avro-serde"  % V.CONFLUENT
  )

  lazy val testDependencies = Seq(
    "org.scalatest" %% "scalatest" % V.SCALATEST % Test
  )
}
