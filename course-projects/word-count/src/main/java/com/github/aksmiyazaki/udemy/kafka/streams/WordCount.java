package com.github.aksmiyazaki.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class WordCount {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "WordCount");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "wordcount-application" + (ThreadLocalRandom.current().nextInt(1, 1001)));

        StreamsBuilder builder = new StreamsBuilder();

        // 1 - Stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // 2 - map values to lowercase
        KTable<String, Long> wordCounts =
                wordCountInput.mapValues(value -> value.toLowerCase())
                        .flatMapValues(value -> Arrays.asList(value.split(" "))).selectKey((key, value) -> value)
                        .groupByKey()
                        .count(Materialized.as("Count"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Prints the topology
        System.out.println(streams.toString());

        // Adds shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
