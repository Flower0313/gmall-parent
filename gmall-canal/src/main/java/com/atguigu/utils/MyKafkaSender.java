package com.atguigu.utils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

/**
 * @ClassName gmall-parent-MyKafkaSender
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月30日17:42 - 周二
 * @Describe kafka发送者
 */
public class MyKafkaSender {
    public static KafkaProducer<String, String> kafkaProducer = null;

    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        //序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {//若生产者为空就调用此方法
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic, msg));
    }



}
