package com.github.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String topic = "first_topic";

        //Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create Consumer

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(properties);

        // Subscribe consumer to the topic(s)

        TopicPartition topicPartition = new TopicPartition(topic,0);

        consumer.assign(Arrays.asList(topicPartition));

        consumer.seek(topicPartition,5);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // Poll for the data
        while (keepOnReading)
        {
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record : records) {
                numberOfMessagesReadSoFar++;
                logger.info("Key : "+record.key() + " Value : "+ record.value() +
                            " Partition : "+ record.partition());

                if(numberOfMessagesReadSoFar>numberOfMessagesToRead)
                {
                    keepOnReading=false;
                    break;
                }
            }

        }

        

    }

}
