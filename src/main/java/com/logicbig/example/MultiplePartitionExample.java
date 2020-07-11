package com.logicbig.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiplePartitionExample {
    private static int PARTITION_COUNT = 2;
    private static String TOPIC_NAME = "example-topic-2";
    private static int MSG_COUNT = 4;

    public static void main(String[] args) throws Exception {

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(MultiplePartitionExample::startConsumer);
        executorService.execute(MultiplePartitionExample::startConsumer2);
        executorService.execute(MultiplePartitionExample::sendMessages);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }
    //create different consumer for each partition
    private static void startConsumer() {
        KafkaConsumer<String, String> consumer = Configuration.createConsumer(TOPIC_NAME);
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, 0)));
        int numMsgReceived = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                numMsgReceived++;
                System.out.printf("thread1 consumed: key = %s, value = %s, partition id= %s, offset = %s%n",
                        record.key(), record.value(), record.partition(), record.offset());
            }
            consumer.commitSync();
            if (numMsgReceived == MSG_COUNT) {
                break;
            }
        }
    }
    //create different consumer for each partition
    private static void startConsumer2() {
        KafkaConsumer<String, String> consumer = Configuration.createConsumer(TOPIC_NAME);
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, 1)));
        int numMsgReceived = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                numMsgReceived++;
                System.out.printf("thread2 consumed: key = %s, value = %s, partition id= %s, offset = %s%n",
                        record.key(), record.value(), record.partition(), record.offset());
            }
            consumer.commitSync();
            if (numMsgReceived == MSG_COUNT * PARTITION_COUNT) {
                break;
            }
        }
    }
//send messages to different partitions
    private static void sendMessages() {
        KafkaProducer producer = Configuration.createProducer();
        for (int i = 0; i < MSG_COUNT; i++) {
            for (int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++) {
                String value = "message-" + i;
                String key = Integer.toString(i);
                System.out.printf("Sending message topic: %s, key: %s, value: %s, partition id: %s%n",
                        TOPIC_NAME, key, value, partitionId);
                producer.send(new ProducerRecord<>(TOPIC_NAME, partitionId, key, value));
            }
        }
    }
}