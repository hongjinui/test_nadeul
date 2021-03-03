package com.simple.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo_2 {

    public static void main(String[] args) {
        //여기도 추가여
        Logger logger = LoggerFactory.getLogger(ConsumerDemo_2.class.getName());

        //declare
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my-first-application";

        //create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //create consumer
        KafkaConsumer consumer = new KafkaConsumer(properties);

        //subscribe topic
//        consumer.subscribe(Collections.singleton(topic));
        consumer.subscribe(Arrays.asList(topic));

        while(true) {
            //create poll records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
//            logger.info("records size : " + records.count());
            //log records
            for (ConsumerRecord<String, String> record : records) {
                logger.info("KEY : " + record.key());
                logger.info("VALUE : " + record.value());
                logger.info("partition" + record.partition());
                logger.info("offset" + record.offset());
            }
        }
    }
}
