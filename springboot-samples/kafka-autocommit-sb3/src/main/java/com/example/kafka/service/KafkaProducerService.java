package com.example.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

	private final KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	// Method to send a message to Kafka
	public void sendMessage(String message) {
		kafkaTemplate.send("my-topic", message);
		System.out.println("Message sent: " + message);
	}

	// Method to send a message with key (for partition control)
	public void sendMessageWithKey(String key, String message) {
		kafkaTemplate.send("my-topic", key, message);
		System.out.println("Message sent with key: " + key + ", message: " + message);
	}
}
