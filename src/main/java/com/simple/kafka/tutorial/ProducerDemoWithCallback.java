package com.simple.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
//        System.out.println("hello world!");

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getName());
        String bootstrapServer = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++) {

            // create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world_" + Integer.toString(i));

            // send data - asynchronous
            kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("kafka producer metadata : \n" +
                                "topic :" + metadata.topic() + "\n" +
                                "partition : " + metadata.partition() + "\n" +
                                "offset : " + metadata.offset() + "\n" +
                                "timestamp : " + metadata.timestamp()
                        );
                    } else {
                        exception.getStackTrace();
                    }
                }
            });
        }
        // flush data
        kafkaProducer.flush();
        // flush and close producer
        kafkaProducer.close();



    }
}
