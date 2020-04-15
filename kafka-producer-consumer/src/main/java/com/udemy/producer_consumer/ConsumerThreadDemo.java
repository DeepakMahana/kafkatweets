package com.udemy.producer_consumer;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerThreadDemo {
	
	public static void main(String[] args) {
		new ConsumerThreadDemo().run();
	}
	
	private ConsumerThreadDemo(){
		
	}
	
	private void run(){
		
		Logger logger = LoggerFactory.getLogger(ConsumerThreadDemo.class.getName());
		
		String bootStrapServers = "127.0.0.1:9092";
		String groupId = "my-application";
		String topic = "first_topic";
		
		CountDownLatch latch = new CountDownLatch(1);
		
		logger.info("Creating the Consumer");
		Runnable myConsumerRunnable = new ConsumerRunnable(bootStrapServers, groupId, topic, latch);
		
		// Start the thread
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();
		
		// Add a Shutdown Hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			logger.info("Caught Shutdown Hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
		}));
		
		try{
			latch.await();
		}catch(InterruptedException e){
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}
	
	
	public class ConsumerRunnable implements Runnable{
		
		Logger logger = LoggerFactory.getLogger(ConsumerThreadDemo.class.getName());
		
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		
		public ConsumerRunnable(String bootstrapServers,
							  String groupId,
							  String topic,
							  CountDownLatch latch){
			this.latch = latch;
			
			// Create Consumer Config
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
						
			// Create Consumer
			consumer = new KafkaConsumer<String, String>(properties);
			
			// Subscribe Consumer To our Topic
			consumer.subscribe(Arrays.asList(topic));
		}
		
		@Override
		public void run() {
			// poll for new data
			
			try{
				
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					
					for(ConsumerRecord<String, String> record: records) {
						logger.info("Key: " + record.key() + ", Value: " + record.value());
						logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
					}
				}
			} catch(WakeupException e){
				logger.info("Received Shutdown Signal !");
			} finally {
				consumer.close();
				// Tell our main code we're done with the consumer
				latch.countDown();
			}
			
		}
		
		public void shutdown(){
			// the wakeup() is a special method to interrupt consumer.poll()
			// it will throw the exception WakeUpException
			consumer.wakeup();
		}
		
	}

}
