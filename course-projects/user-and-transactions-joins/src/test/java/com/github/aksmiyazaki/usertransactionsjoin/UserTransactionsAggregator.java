package com.github.aksmiyazaki.usertransactionsjoin;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class UserTransactionsAggregator {
    static final String INPUT_USERS_TOPIC = "example-users";
    static final String INPUT_TRANSACTIONS_TOPIC = "example-transactions";
    static final String OUTPUT_INNER_JOIN_TOPIC = "inner-joined-transactions";
    static final String OUTPUT_LEFT_JOIN_TOPIC = "left-joined-transactions";
    static final String SCHEMA_REGISTRY_URL =  "http://localhost:8081";


    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();

        Map<String, String> serdeConfigs = Collections
                .singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        SCHEMA_REGISTRY_URL);

        SpecificAvroSerde<User> userSerde = new SpecificAvroSerde();
        SpecificAvroSerde<Transaction> transactionSerde = new SpecificAvroSerde();
        SpecificAvroSerde<CompleteTransaction> resultSerde = new SpecificAvroSerde();

        userSerde.configure(serdeConfigs, false);
        transactionSerde.configure(serdeConfigs, false);
        resultSerde.configure(serdeConfigs, false);

        GlobalKTable<Integer, User> usersTable = builder.globalTable(
                INPUT_USERS_TOPIC,
                Consumed.with(Serdes.Integer(), userSerde, null, Topology.AutoOffsetReset.EARLIEST)
        );

        KStream<Integer, Transaction> transactionsStream = builder.stream(
                INPUT_TRANSACTIONS_TOPIC,
                Consumed.with(Serdes.Integer(), transactionSerde, null, Topology.AutoOffsetReset.EARLIEST)
        );

        KStream<Integer, CompleteTransaction> innerStream = transactionsStream.join(usersTable,
                (transactionKey, transactionValue) -> transactionValue.getUserId(),
                (transaction, user) -> generateCompleteTransactionFromSources(user, transaction)
        );

        innerStream.to(OUTPUT_INNER_JOIN_TOPIC, Produced.with(Serdes.Integer(), resultSerde));

        Properties props = defineProperties();
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Properties defineProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "usertransactionjoiner");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "10");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        return props;
    }

    public static CompleteTransaction generateCompleteTransactionFromSources(User user, Transaction trans) {
        CompleteTransaction complete = new CompleteTransaction(trans.getId(),
                user.getId(),
                user.getName(),
                trans.getTransactionValue(),
                trans.getTransactionTs(),
                user.getLastUpdated());
        return complete;
    }


}
