package com.udemy.producer_consumer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

	public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
		String bootstrapServers = "127.0.0.1:9092";
		
		// Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create The Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		// Create A Producer Record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");
		
		// Send Data - Async
		// producer.send(record);	- Normal
		
		// Using Callback
		producer.send(record, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				// executes every time a record is successfully sent or an exception is thrown
				if(e == null){
					// The record was successfull sent
					logger.info("Received new metadata. \n" +
								"Topic: " + recordMetadata.topic() + "\n" +
								"Partition: " + recordMetadata.partition() + "\n" +
								"Offset: " + recordMetadata.offset() + "\n" +
								"Timestamp: " + recordMetadata.timestamp() + "\n"
					          );
				} else {
					logger.error("Error While Producing", e);
				}
				
			}
		});
		

		// flush data
		producer.flush();
		// close producer
		producer.close();
		
	}

}
