package com.github.aksmiyazaki.moneytransactions

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.lang.Thread.sleep
import java.util.Properties
import scala.util.Random
import java.time
import java.time.{LocalDateTime, ZoneId}

object ProducerApp extends App {

  def defineProperties(): Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.RETRIES_CONFIG, "10")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    props.put("schema.registry.url", "http://127.0.0.1:8081")
    props
  }

  def generateTransaction(lowerBound: Int, upperBound: Int) = {
    val rnd = new Random()
    val transactionIntegerValue = lowerBound + rnd.nextInt(upperBound - lowerBound + 1)
    val transactionDecimalValue = (rnd.nextFloat() * 100).toInt
    val ts = LocalDateTime.now().atZone(ZoneId.of("UTC")).toInstant

    Transaction(f"${transactionIntegerValue}.${transactionDecimalValue}", "deposit", ts)
  }

  def producerCallback: Callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
      Option(exception)
        .map(error => println("fail to produce a record due to: ", error))
        .getOrElse(println(s"Successfully produce a new record to kafka: ${
          s"topic: ${metadata.topic()}, partition: ${metadata.partition()}, offset: ${metadata.offset()}"
        }"))
  }



  val producer: KafkaProducer[String, Transaction] = new KafkaProducer(defineProperties)
  val topic = "money-transactions-input"
  val messagesPerSecond = 1 to 2
  val rounds = 1 to 10
  val customers = Seq("John")//, "Maria", "Ricardo", "Jose", "Patty", "Carlos")

  for (i <- rounds) {
    for (genMsg <- messagesPerSecond)
      customers.map{ customer =>
        val record = new ProducerRecord[String, Transaction](topic,
          customer,
          generateTransaction(1, 100))

        producer.send(record, producerCallback)
      }

    producer.flush()
    sleep(10 * 1000)
  }
  producer.close()
}
