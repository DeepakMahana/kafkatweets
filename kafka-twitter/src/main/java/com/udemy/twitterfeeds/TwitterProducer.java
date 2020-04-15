package com.udemy.twitterfeeds;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class TwitterProducer {
	
	String consumerKey = "65772873842O26G6nRVMwoXWuQrjRBPVaL";
	String consumerSecret = "12312YTSdhsd3E2qIAqCrjARus8qwbQyQgQLi9jkTLUyEvaEjY1OH9xic4a8LW";
	String token = "672637105833071-nwD3Lau9cQaRVYRwIVX6QMNJq5inGrOscq1iYGCOzat";
	String secret = "287359FMZHSsNg4mFWaH0UawwVoMxRJYhSkXAokmZSBSaXjk6634axfztAx";
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	List<String> terms = Lists.newArrayList("coronavirus");
	
	public TwitterProducer(){}
	
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run(){
		
		/** Set Up blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		// Create a Twitter Client
		Client client = createTwitterClient(msgQueue);
		client.connect();
		
		// Create a Kafka Producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		// Add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Stopping Application");
			logger.info("Shutting Down Client from Twitter");
			client.stop();
			logger.info("Closing Producer");
			producer.close();
			logger.info("Done !");
		}));
		
		// Loop to send tweets to kafka on different threads
		while(!client.isDone()){
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			}catch(InterruptedException e){
				e.printStackTrace();
				client.stop();
			}
			if(msg != null){
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback(){

					@Override
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						if(e != null){
							logger.error("Something bad happened", e);
						}	
					}
					
				});
			}
		}
		
		logger.info("End of Subscription");
	}
	
	public Client createTwitterClient(BlockingQueue<String> msgQueue){
		
		/** Declare the host to connect, the endpoint, and authentication */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		// Optional: setup some followings and track terms
		hosebirdEndpoint.trackTerms(terms);
		
		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		
		// Build Client
		ClientBuilder builder = new ClientBuilder()
				.name("HoseBird-Client-001")
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		
		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer(){
		
		String bootstrapServers = "127.0.0.1:9092";
		
		// Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create a safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		
		// High Throughput producer (at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size
		
		// Create The Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		return producer;
	}
}
