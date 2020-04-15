# kafkatweets

Apache Kafka Tweets Streaming to ElascticSearch

* Generated basic Kafka producer-consumer to exchange data between them.
* Created a Twitter Producer to get Tweets from Twitter API into Kafka.
* Created a Consumer to poll twitter tweets from Kafka into ElasticSearch.
* Created a filter to push tweets only if followers count is more than 10k using Kafka streams. 
