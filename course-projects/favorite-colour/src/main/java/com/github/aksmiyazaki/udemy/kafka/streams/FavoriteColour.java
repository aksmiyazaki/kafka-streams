package com.github.aksmiyazaki.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class FavoriteColour {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "FavoriteColour");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, "favcolour" + (ThreadLocalRandom.current().nextInt(1, 1001)));

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> streamInput = builder.stream("favorite-colour-input");
        String[] allowedValues = {"red", "blue", "green"};


        streamInput.filter((key, value) -> value.contains(","))
                .mapValues(value -> value.toLowerCase())
                .map((key, value) -> KeyValue.pair(value.split(",")[0], value.split(",")[1]))
                .filter((key, value) -> Arrays.asList(allowedValues).contains(value))
                .to("favorite-colour-cleaned-stream");

        KTable<String, String> cleanedTable = builder.table("favorite-colour-cleaned-stream");

        cleanedTable.groupBy((key, value) -> KeyValue.pair(value, value))
                .count()
                .toStream()
                .to("favorite-colour-output-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Prints the topology
        System.out.println(streams.toString());

        // Adds shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
