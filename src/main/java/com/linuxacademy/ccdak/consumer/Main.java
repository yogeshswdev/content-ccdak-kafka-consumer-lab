package com.linuxacademy.ccdak.consumer;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Main {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "group1");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        java.nio.file.Path outputFile = java.nio.file.Paths.get("/workspaces/content-ccdak-kafka-consumer-lab/out/output.dat");
        try {
            java.nio.file.Files.createDirectories(outputFile.getParent());
        } catch (IOException e) {
            throw new RuntimeException("Could not create output directory: " + outputFile.getParent(), e);
        }

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             BufferedWriter writer = java.nio.file.Files.newBufferedWriter(outputFile,
                     java.nio.file.StandardOpenOption.CREATE,
                     java.nio.file.StandardOpenOption.APPEND)) {
            consumer.subscribe(Arrays.asList("inventory_purchases"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String recordString = "key=" + record.key() + ", value=" + record.value() + ", topic=" + record.topic() + ", partition=" + record.partition() + ", offset=" + record.offset();
                    System.out.println(recordString);
                    writer.write(recordString + "\n");
                }
                consumer.commitSync();
                writer.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
