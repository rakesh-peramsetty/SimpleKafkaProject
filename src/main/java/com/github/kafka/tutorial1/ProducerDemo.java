package com.github.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer

        KafkaProducer producer = new KafkaProducer(properties);

        // Create Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<String,String>("first_topic","Hello Rakesh");

        //Send message
        producer.send(record);
        //as Send method is asynchronous call, producer is getting closed before the message is sent.
        // So flush and close the producer
        //flush the data
        producer.flush();
        //flush and close the producer
        producer.close();
        

    }

}
