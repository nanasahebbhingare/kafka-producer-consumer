package com.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerKeyValueDemo {
	final static Logger log = LoggerFactory.getLogger(KafkaProducerKeyValueDemo.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// Create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// Create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		for (int i = 0; i < 5; i++) {
			String key = "id_" + i;
			String value = "Hello Time_" + i;
			// Create produce record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", key, value);
			// Send data --Asynchronous
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					// If Error is occur or information showing.
					if (e == null) {
						log.info("\n Topic: {} \n Partition: {} \n Offset: {}", metadata.topic(), metadata.partition(),
								metadata.offset());
					} else {
						log.error("Exception: {}", e.getMessage(), e);
					}
				}
			}).get();
		}
		// Flush data
		producer.flush();
		// Close data
		producer.close();
	}
}
