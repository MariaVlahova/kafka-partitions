package com.logicbig.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.util.Arrays;
import java.util.Properties;

public class Configuration {
    public static final String BROKERS = "localhost:9092";

    public static KafkaProducer createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKERS);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, String> createConsumer(String topicName) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BROKERS);
        props.setProperty("group.id", "testGroupMimi");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //don't call consumer#subscribe() if you will use manuall assign
        //assigning partition-id ex:  consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, partition)));
        //consumer.subscribe(Arrays.asList(topicName));
        return consumer;
    }
}