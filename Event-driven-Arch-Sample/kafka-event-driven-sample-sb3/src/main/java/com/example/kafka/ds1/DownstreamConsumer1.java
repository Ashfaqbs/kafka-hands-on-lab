package com.example.kafka.ds1;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class DownstreamConsumer1 {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(topics = "main-topic", groupId = "ds1-group")
	public void consume(String message) {
		System.out.println("DS1 received: " + message);
		// Simulate processing logic
		kafkaTemplate.send("ack-topic", "ds1-ack:Success for message - " + message);
	}
}
