package com.github.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer

        KafkaProducer producer = new KafkaProducer(properties);

        for (int counter = 0; counter<10;counter++)
        {
            String topic = "first_topic";
            String value = "Hello Rakesh"+Integer.valueOf(counter);
            String key = "id_"+Integer.valueOf(counter);
            // Create Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic,key,value);

            //Send message
            producer.send(record,(recordMetadata, e) -> {
                if(e == null)
                {
                    logger.info("$$$$$$$$$$$$$$$$$");
                    logger.info("Topic Name: "+ recordMetadata.topic() + "\n"+
                            "Offset Value: "+ recordMetadata.offset() +"\n"+
                            "Partition: " + recordMetadata.partition()  +"\n"+
                            "Key: "+ key);
                    logger.info("$$$$$$$$$$$$$$$$$");
                }else
                {
                    logger.error("Error in sending the data");
                }

            });
        }

        //as Send method is asynchronous call, producer is getting closed before the message is sent.
        // So flush and close the producer
        //flush the data
        producer.flush();
        //flush and close the producer
        producer.close();


    }

}
