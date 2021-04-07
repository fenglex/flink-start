package com.fenglex.kafka;

import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.unit.DataUnit;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.RandomUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author haifeng
 * @version 1.0
 * @date 2021/3/29 10:39
 */
public class Producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "fenglex.com:8092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 100000);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        int length = 20;
        List<String> codes = new ArrayList<String>(length);
        for (int i = 0; i < length; i++) {
            codes.add("code_00" + i);
        }
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        int startDay = 20210101;
        DateTime startDate = DateUtil.parse(String.valueOf(startDay), "yyyyMMdd");
        String topic = "ser_topic";
        while (true) {
            DateTime offsetDay = DateUtil.offsetDay(startDate, 1);
            ThreadUtil.sleep(1, TimeUnit.SECONDS);
            int i = RandomUtil.randomInt(0, 20);
            String code = codes.get(i);
            String formatDay = DateUtil.format(offsetDay, "yyyyMMdd");
            String value = String.valueOf(RandomUtil.randomInt(5000));
            String line = String.format("%s|%s|%s", code, formatDay, value);
            System.out.println("send_msg:" + line);
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, line);
            kafkaProducer.send(msg);
        }
        //IoUtil.copy(kafkaProducer);
        //kafkaProducer.close();
    }

}
