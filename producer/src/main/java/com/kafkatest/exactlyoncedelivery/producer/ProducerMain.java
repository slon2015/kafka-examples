package com.kafkatest.exactlyoncedelivery.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerMain {
    public static void main(String[] args) {
        final String DOCKER_HOST = System.getenv("DOCKER_HOST_IP");

        Properties props = new Properties();
        props.put("bootstrap.servers", DOCKER_HOST + ":9092," + DOCKER_HOST + ":9093," + DOCKER_HOST + ":9094");
        props.put("enable.idempotence", true);
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        for (String record : Objects.requireNonNull(args[0]).split(" ")) {
            producer.send(new ProducerRecord<>("topic1", record));
        }
        producer.flush();
        producer.close();
    }
}
