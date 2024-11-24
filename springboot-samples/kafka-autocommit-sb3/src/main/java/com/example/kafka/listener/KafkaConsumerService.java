package com.example.kafka.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

	// Listener 1 for consuming messages
	@KafkaListener(topics = "my-topic", groupId = "group1")
	public void listenGroup1(String message, Acknowledgment acknowledgment) {
		System.out.println("Group1 Received message: " + message);
		// Manually acknowledge the message
		acknowledgment.acknowledge();
		System.out.println("Group1 Offset manually committed");
	}

	// Listener 2 for consuming messages
	@KafkaListener(topics = "my-topic", groupId = "group2")
	public void listenGroup2(String message) {
		System.out.println("Group2 Received message: " + message);
	}
}
