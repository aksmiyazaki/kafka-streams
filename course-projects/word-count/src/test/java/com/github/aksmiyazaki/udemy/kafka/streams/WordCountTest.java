package com.github.aksmiyazaki.udemy.kafka.streams;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WordCountTest {

    TopologyTestDriver testDriver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, Long> outputTopic;

    @Before
    public void setUpTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dummyid");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        WordCount wordcount = new WordCount();
        Topology topology = wordcount.createTopology();

        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic(WordCount.INPUT_TOPIC,
                new StringSerializer(),
                new StringSerializer());

        outputTopic = testDriver.createOutputTopic(WordCount.OUTPUT_TOPIC,
                new StringDeserializer(),
                new LongDeserializer());
    }

    @After
    public void closeDriver() {
        testDriver.close();
    }


    @Test
    public void dumyTest() {
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    @Test
    public void makeSureCountIsCorrect() {
        inputTopic.pipeInput("Hello kafka streams");
        Map<String, Long> kv = outputTopic.readKeyValuesToMap();
        assertTrue(Optional.ofNullable(kv.get("hello")).get() == 1L);
        assertTrue(Optional.ofNullable(kv.get("kafka")).get() == 1L);
        assertTrue(Optional.ofNullable(kv.get("streams")).get() == 1L);
    }


    @Test
    public void makeSureCountIsCorrectHavingMoreThanASingleMessage() {
        inputTopic.pipeInput("Hello kafka streams");
        inputTopic.pipeInput("Hello kafka streams streams StReAmS");
        Map<String, Long> kv = outputTopic.readKeyValuesToMap();
        assertTrue(Optional.ofNullable(kv.get("hello")).get() == 2L);
        assertTrue(Optional.ofNullable(kv.get("kafka")).get() == 2L);
        assertTrue(Optional.ofNullable(kv.get("streams")).get() == 4L);
    }

}
