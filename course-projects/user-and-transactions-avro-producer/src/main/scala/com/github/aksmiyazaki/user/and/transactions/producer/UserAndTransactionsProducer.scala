package com.github.aksmiyazaki.user.and.transactions.producer

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.{LocalDateTime, ZoneId}
import java.util.Properties

object UserAndTransactionsProducer extends App {
  val props = defineProperties()

  val usersTopic = "example-users"
  val transactionsTopic = "example-transactions"

  val userProducer: KafkaProducer[Int, User] = new KafkaProducer(defineProperties)
  val transactionProducer: KafkaProducer[Long, Transaction] = new KafkaProducer(defineProperties)

  val user1 = generateUser(1, "Jose")
  val user2 = generateUser(2, "Maria")
  val user3 = generateUser(3, "Geraldo")
  val user4 = generateUser(4, "Pedro")

  userProducer.send(new ProducerRecord[Int, User](usersTopic, user1.id, user1), producerCallback)
  userProducer.send(new ProducerRecord[Int, User](usersTopic, user2.id, user2), producerCallback)
  userProducer.send(new ProducerRecord[Int, User](usersTopic, user3.id, user3), producerCallback)
  userProducer.send(new ProducerRecord[Int, User](usersTopic, user4.id, user4), producerCallback)

  transactionProducer.send(
    new ProducerRecord[Long, Transaction](transactionsTopic, 1L, generateTransaction(1, "22.5")),
    producerCallback)

  transactionProducer.send(
    new ProducerRecord[Long, Transaction](transactionsTopic, 2L, generateTransaction(2, "1.0")),
    producerCallback)

  transactionProducer.send(
    new ProducerRecord[Long, Transaction](transactionsTopic, 3L, generateTransaction(3, "2.5")),
    producerCallback)

  transactionProducer.send(
    new ProducerRecord[Long, Transaction](transactionsTopic, 4L, generateTransaction(99, "22.5")),
    producerCallback)

  userProducer.flush()
  transactionProducer.flush()
  userProducer.close()
  transactionProducer.close()

  def generateTransaction(user_id: Int, value: String): Transaction = {
    Transaction(user_id, value, LocalDateTime.now().atZone(ZoneId.of("UTC")).toInstant)
  }

  def generateUser(id: Int, name: String): User = {
    User(id, name, LocalDateTime.now().atZone(ZoneId.of("UTC")).toInstant)
  }

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

  def producerCallback: Callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
      Option(exception)
        .map(error => println("fail to produce a record due to: ", error))
        .getOrElse(println(s"Successfully produce a new record to kafka: ${
          s"topic: ${metadata.topic()}, partition: ${metadata.partition()}, offset: ${metadata.offset()}"
        }"))
  }



}
