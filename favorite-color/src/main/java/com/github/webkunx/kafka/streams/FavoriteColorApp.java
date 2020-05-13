package com.github.webkunx.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Properties;

public class FavoriteColorApp {
    public static void main(String[] args) {
        Properties config = new Properties();

        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("favorite-color-input");

        KStream<String, String> kTable = stream
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0])
                .mapValues((key, value) -> value.split(",")[1].toLowerCase())
                .filter((key, value) -> value.equals("green") || value.equals("blue") || value.equals("red"));

        kTable.to("user-keys-and-colors");
        KTable<KeyValue<String, String>, Long> favoriteColors = kTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Named.as("CountsByColors"));


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }
}
