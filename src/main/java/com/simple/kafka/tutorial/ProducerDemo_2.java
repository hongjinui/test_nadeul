package com.simple.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo_2 {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemo_2.class.getName());

        //declare
        String bootstrapSever = "127.0.0.1:9092";
        String topic = "first_topic";

        //create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapSever);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer producer = new KafkaProducer(properties);

        for(int i=0; i<100; i++) {

            String key = "id_" + Integer.toString(i);
            String value= "good bye sunday_" + Integer.toString(i);

            //create record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("KEY :" + key);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {

                    if (exception == null) {
                        logger.info("topic :" + metadata.topic());
                        logger.info("partition :" + metadata.partition());
                        logger.info("offset:" + metadata.offset());
                        logger.info("timestamp:" + metadata.timestamp());
                    } else {
                        exception.getStackTrace();
                    }

                }
            });
        }
        //flush
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
