package com.fenglex.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Properties;

/**
 * @author haifeng
 * @version 1.0
 * @date 2021/3/31 16:48
 */
public class Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "fenglex.com:8092");
        props.put("group.id", "aaa");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String,String>(props);
        String topic="ser_topic";
        kafkaConsumer.subscribe(Collections.singleton(topic));
        //kafkaConsumer.seekToBeginning(Collections.singletonList(new TopicPartition(topic, 0)));
        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(3);
            for (ConsumerRecord<String, String> record : poll) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }
}
