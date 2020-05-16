package com.github.webkunx.kafka.streams;

import com.github.webkunx.kafka.streams.basics.StreamsStarterApp;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class WordCountAppTest {

    StringSerializer stringSerializer = new StringSerializer();
    private ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>("input-topic", stringSerializer, stringSerializer);

    TopologyTestDriver testDriver;

    @Before
    public void setUpTestDriver() {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-word");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        Topology topology = StreamsStarterApp.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    public void pushNewInputRecord(String value) {
        testDriver.pipeInput(recordFactory.create("word-count-input", null, value));
    }

    @Test
    public void dummyTest() {
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    public ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    @Test
    public void ensureCountsAreCorrect() {
        String firstExample = "testing app";
        pushNewInputRecord(firstExample);
        OutputVerifier.compareKeyValue(readOutput(), "testing", 1l);
        OutputVerifier.compareKeyValue(readOutput(), "app", 1l);

        assertEquals(readOutput(), null);

        String secondExample = "i m testing kafka app again";
        pushNewInputRecord(secondExample);
        OutputVerifier.compareKeyValue(readOutput(), "i", 1l);
        OutputVerifier.compareKeyValue(readOutput(), "m", 1l);
        OutputVerifier.compareKeyValue(readOutput(), "testing", 2l);
        OutputVerifier.compareKeyValue(readOutput(), "kafka", 1l);

        OutputVerifier.compareKeyValue(readOutput(), "app", 2l);
        OutputVerifier.compareKeyValue(readOutput(), "again", 1l);

        assertEquals(readOutput(), null);

    }
}
