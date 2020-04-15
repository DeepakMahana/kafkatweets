

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

	
public class ElasticSearchConsumer {
	
	public static RestHighLevelClient createClient() {
		
		// Bonsai Credentials for ElasticSearch
		String hostname = "kafka-project-2163684625.ap-southeast-2.bonsaisearch.net";
		String username = "2nnzjzrbigxaysz";
		String password = "b7an46ks6ahnalk";
		
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		
		RestClientBuilder builder = RestClient.builder(
				new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	
	public static KafkaConsumer<String, String> createConsumer(String topic) {
		
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		
		String bootStrapServers = "127.0.0.1:9092";
		String groupId = "kafka-elasticsearch";
		
		// Create Consumer Config
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		
		// Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		
		return consumer;
		
	}
	
	private static JsonParser jsonParser = new JsonParser();
	private static String extractIdFromTweet(String tweetJson){
		
		// Gson Library
		return jsonParser.parse(tweetJson)
						 .getAsJsonObject()
						 .get("id_str")
						 .getAsString();
	}

	public static void main(String[] args) throws IOException {
		
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
		
		// Poll For new Data
		while(true){
			
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			Integer recordCount = records.count();
			logger.info("Received " + recordCount + " records");
			
			// Create Bulk Request
			BulkRequest bulkRequest = new BulkRequest();
			
			for(ConsumerRecord<String, String> record: records){
				
				
				try{
				
				/** 2 Strategies to get a unique ID to make the consumer Idempotent */
				
				/** 1st Strategy - Kafka Generic ID */
				// String id = record.topic() + "_" + record.partition() + "_" + record.offset();
				
				/** 2nd Strategy - Twitter Feed Specific ID */
				String id = extractIdFromTweet(record.value());
				
				// Insert data into ES
				IndexRequest indexRequest = new IndexRequest(
						"twitter",
						"tweets",
						id
				).source(record.value(), XContentType.JSON);
				
				// Add IndexRequests to Bulk Request for batching
				bulkRequest.add(indexRequest);
				
				/** If Single Request*/
//				IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//				logger.info(indexResponse.getId());
				
				}catch(NullPointerException e){
					logger.warn("Skipping Bad Data" + record.value());
				}
			}
			
			if(recordCount > 0){
				BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("Committing Offsets");
				consumer.commitSync();
				logger.info("Offsets have been committed");
			}
			
			
		}
		
		// Close the client gracefully
		// client.close();

	}

}
