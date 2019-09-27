package com.kafkatest.exactlyoncedelivery.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.*;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class ConsumerMain {
    public static void main(String[] args) throws IOException {
        final String DOCKER_HOST = System.getenv("DOCKER_HOST_IP");

        File stateFile = new File("../state");
        if (!stateFile.exists()) {
            stateFile.createNewFile();
        }

        List<String> state = new LinkedList<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(stateFile))) {
            reader.lines().forEach(state::add);
        }

        state.stream().forEach(System.out::println);
        System.out.println("\n\t\tReading from Kafka\n\n");

        Properties props = new Properties();
        props.put("bootstrap.servers", DOCKER_HOST + ":9092," + DOCKER_HOST + ":9093," + DOCKER_HOST + ":9094");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton("topic1"));
        while (true) {
            int offset = state.stream().map(s -> s.split(":")[0]).mapToInt(Integer::parseInt).max().orElse(0);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                if (offset > record.offset()) {
                    continue;
                }
                String stateRecord = record.offset() + ":" + record.value();
                System.out.println(stateRecord);
                state.add(stateRecord);
            }
            saveState(stateFile, state);
            consumer.commitSync();
        }

    }

    public static void saveState(File file, List<String> state) {
        StringBuilder stateSerializer = new StringBuilder();
        state.stream().forEach(s -> stateSerializer.append(s + '\n'));
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            writer.write(stateSerializer.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
