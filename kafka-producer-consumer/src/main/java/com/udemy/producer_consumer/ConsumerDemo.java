package com.udemy.producer_consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
		
		String bootStrapServers = "127.0.0.1:9092";
		String groupId = "my-application";
		String topic = "first_topic";
		
		// Create Consumer Config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		// Subscribe Consumer to our
		consumer.subscribe(Arrays.asList(topic));
		
		// Poll For new Data
		while(true){
			
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record: records){
				logger.info("Key: " + record.key() + ", Value: " + record.value());
				logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
			}
			
		}
		
	}

}
