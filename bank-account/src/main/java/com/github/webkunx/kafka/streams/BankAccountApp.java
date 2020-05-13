package com.github.webkunx.kafka.streams;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Properties;
import java.util.Random;

public class BankAccountApp {


    public static void main(String[] args) throws InterruptedException {
        Random rand = new Random();
        final Logger logger = LoggerFactory.getLogger(BankAccountApp.class);

        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerConfig);
        // while true
        logger.info("Started producing");

        while (true) {
            // sleep for rand time
            try {

                int sleepTimeMs = rand.nextInt(2000);

                Thread.sleep(sleepTimeMs);
                // generate random int from 1 t0 1000000
                int randomInt = rand.nextInt((1000000 - 1) + 1) + 1;

                // get rand name from arr
                String[] names = new String[]{"John", "Oleg", "Tolya"};
                String name = names[rand.nextInt(names.length)];
                // get time
                Timestamp time = new Timestamp(System.currentTimeMillis());
                // create json

                String json = new JSONObject()
                        .put("name", name)
                        .put("amount", randomInt)
                        .put("time", time)
                        .toString();
                logger.info(json);
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>("bank-transactions", name, json);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Successfully send data");
                        } else {
                            logger.error("Error while producing data", e);
                        }
                    }
                });
                producer.flush();
            } catch (Exception e) {
                logger.error(String.valueOf(e));
            }
            // send
            // log success
        }


    }
}


