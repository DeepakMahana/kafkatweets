package com.udemy.filtertweets;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;



public class StreamsFilterTweets {
	
	private static JsonParser jsonParser = new JsonParser();
	private static Integer extractUserFollowersInTweet(String tweetJson){
		// Gson Library
		
		try{
			return jsonParser.parse(tweetJson)
					 .getAsJsonObject()
					 .get("user")
					 .getAsJsonObject()
					 .get("followers_count")
					 .getAsInt();
		}catch(NullPointerException e){
			return 0;
		}
	}
	
	public static void main(String[] args){
		
		String bootstrapServers = "127.0.0.1:9092";
		
		// Create Properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		// Create Topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		// Input Topic
		KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
		KStream<String, String> filteredStream = inputTopic.filter(
				(k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000
		);
		filteredStream.to("important_tweets");
		
		// Build The Topology
		KafkaStreams kafkaStreams = new KafkaStreams(
				streamsBuilder.build(),
				properties
		); 
		
		// Start our streams application
		kafkaStreams.start();
	}
}
