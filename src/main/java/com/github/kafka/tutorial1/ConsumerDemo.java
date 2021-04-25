package com.github.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        //Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-first-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create Consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);

        // Subscribe consumer to the topic(s)

        consumer.subscribe(Arrays.asList("first_topic"));

        // Poll for the data
        while (true)
        {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record : records) {

                logger.info("Key : "+record.key() + " Value : "+ record.value() +
                            " Partition : "+ record.partition());
            }

        }

        

    }

}
