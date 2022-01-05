package com.github.aksmiyazaki.moneytransactions.aggregator

import com.github.aksmiyazaki.moneytransactions.{AccountStatus, Transaction}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{Consumed, Grouped, KStream, Materialized, Produced}

import java.util.Properties
import java.util.Collections

object TransactionsAggregator extends App {
  val KIND_DEPOSIT = "deposit"
  val SCHEMA_REGISTRY_URL = "http://localhost:8081"

  val builder = new StreamsBuilder()

  final val serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL)

  val valueSourceSerde: SpecificAvroSerde[Transaction] = new SpecificAvroSerde
  val valueResultSerde: SpecificAvroSerde[AccountStatus] = new SpecificAvroSerde

  valueSourceSerde.configure(serdeConfig, false)
  valueResultSerde.configure(serdeConfig, false)

  val stream: KStream[String, Transaction] = builder.stream[String, Transaction]("money-transactions-input",
    Consumed.`with`(Serdes.String(), valueSourceSerde, null, Topology.AutoOffsetReset.EARLIEST))


  stream.filter((_, transaction) => transaction.kind == KIND_DEPOSIT)
    .groupByKey(Grouped.`with`(Serdes.String(), valueSourceSerde))
    .aggregate(
      () => generateBlankAccount(),
      (k, v, agg) => treatTransaction(k, v, agg)
    ).toStream
    .to("money-transactions-output", Produced.`with`(Serdes.String(), valueResultSerde))

  val streams = new KafkaStreams(builder.build(), defineProperties)
  streams.cleanUp()
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

  def generateBlankAccount(): AccountStatus = {
    AccountStatus("0.0")
  }

  def defineProperties(): Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "TransactionAggregator")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[SpecificAvroSerde[_ <: SpecificRecord]])
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL)
    props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1")
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

    props
  }

  def treatTransaction(key: String, newTransaction: Transaction, currentStatus: AccountStatus): AccountStatus = {
    println(f"Processing transaction: CUSTOMER $key")
    println(f"Transaction: $newTransaction")
    println(f"Current Status: $currentStatus")

    val depositValue = BigDecimal(f"${newTransaction.value}")
    val accountBalance = BigDecimal(f"${currentStatus.balance}")
    val currentValue = accountBalance + depositValue
    val ts =
      if (newTransaction.timestamp.toEpochMilli > currentStatus.last_transaction_timestamp.toEpochMilli)
        newTransaction.timestamp
      else
        currentStatus.last_transaction_timestamp

    val newStatus = AccountStatus(currentValue.toString(), currentStatus.number_of_deposits + 1, ts)
    println(f"New Status: $newStatus")
    newStatus
  }
}
