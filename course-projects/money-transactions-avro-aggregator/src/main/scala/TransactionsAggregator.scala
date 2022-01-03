package com.github.aksmiyazaki.moneytransactions.aggregator

import com.github.aksmiyazaki.moneytransactions.{AccountStatus, Transaction}
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.kafka.streams.kstream.{KStream}

import java.util.Properties

object TransactionsAggregator extends App {
  val KIND_DEPOSIT = "deposit"


  def defineProperties(): Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "TransactionAggregator")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[SpecificAvroSerde[Transaction]].getCanonicalName)
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1")
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
    props.put("schema.registry.url", "http://127.0.0.1:8081")

    props
  }

  def generateBlankAccount(): AccountStatus = {
    AccountStatus("0.0")
  }

  def treatTransaction(key: String, newTransaction: Transaction, currentStatus: AccountStatus): AccountStatus = {
    println(f"Processing transaction: CUSTOMER $key")
    println(f"Transaction: $newTransaction")
    println(f"Current Status: $currentStatus")

    val depositValue = BigDecimal(f"${newTransaction.value}")
    val accountBalance = BigDecimal(f"${currentStatus.balance}")
    val currentValue = accountBalance + depositValue
    val ts =
      if(newTransaction.timestamp.toEpochMilli > currentStatus.last_transaction_timestamp.toEpochMilli)
        newTransaction.timestamp
      else
        currentStatus.last_transaction_timestamp

    val newStatus = AccountStatus(currentValue.toString(), currentStatus.number_of_deposits + 1, ts)
    println(f"New Status: $newStatus")
    newStatus
  }

  def buildAvroSerializer() = {
    import scala.collection.JavaConversions.mapAsJavaMap
    val valueSpecificAvroSerde = new SpecificAvroSerde[AccountStatus]()
    val serdeConfig = Map("schema.registry.url" -> "http://localhost:8081")
    valueSpecificAvroSerde.configure(mapAsJavaMap(serdeConfig), false)
    valueSpecificAvroSerde
  }

  val builder = new StreamsBuilder()
  val stream: KStream[String, Transaction] = builder.stream("money-transactions-input")

  stream.filter((customer, transaction) => transaction.kind == KIND_DEPOSIT)
    .groupByKey()
    .aggregate(
      () => generateBlankAccount(),
      (k, v, agg) => treatTransaction(k, v, agg)
    ).toStream
    .to("money-transactions-output")

  val streams = new KafkaStreams(builder.build(), defineProperties)
  streams.start()

  // Prints the topology
  System.out.println(streams.toString())

  // Adds shutdown hook
  Runtime.getRuntime().addShutdownHook(new Thread {
    override def run(): Unit = {
      super.run()
      streams.close()
    }
  })
}
