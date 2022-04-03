package com.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerAssignSeek {
	final static Logger log = LoggerFactory.getLogger(KafkaConsumerAssignSeek.class);

	public static void main(String[] args) {
		// Create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// Create the consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// Assign topics
		TopicPartition topicPartition = new TopicPartition("first_topic", 0);
		consumer.assign(Arrays.asList(topicPartition));

		// Seek Topic
		consumer.seek(topicPartition, 15l);
		

		// Poll consumer
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				log.info("Key: " + record.key() + " Value: " + record.value());
				log.info("Partition: " + record.partition() + " Offset: " + record.offset());
				log.info("Partition: " + record.partition() + " Offset: " + record.offset());
			}
		}
	}
}
