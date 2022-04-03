package com.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {
	public static void main(String[] args) {
		// Create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// Create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		// Create produce record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Nana Bhingare");
		// Send data --Asynchronous
		producer.send(record);
		// Flush data
		producer.flush();
		// Close data
		producer.close();
	}
}
