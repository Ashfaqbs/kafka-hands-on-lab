package com.example.kafka.ds2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class DownstreamConsumer2 {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(topics = "main-topic", groupId = "ds2-group")
	public void consume(String message) {
		System.out.println("DS2 received: " + message);
		// Simulate processing logic
		kafkaTemplate.send("ack-topic", "ds2-ack:Success for message - " + message);
	}
}
