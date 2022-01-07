import sbt._

object Dependencies {
  lazy val V = new {
    val AVRO = "1.10.2"
    val KAFKA = "1.0.0"
    val CONFLUENT = "4.0.0"
  }

  lazy val avroDependencies = Seq(
    "org.apache.avro" % "avro" % V.AVRO,
    "org.apache.kafka" % "kafka-clients" % V.KAFKA,
    "io.confluent" % "kafka-avro-serializer"  % V.CONFLUENT
  )
}
