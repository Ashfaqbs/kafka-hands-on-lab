package com.example.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.kafka.service.KafkaProducerService;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

	private final KafkaProducerService kafkaProducerService;

	@Autowired
	public KafkaController(KafkaProducerService kafkaProducerService) {
		this.kafkaProducerService = kafkaProducerService;
	}

	// Endpoint to send a message without a key
	@GetMapping("/publish")
	public String publishMessage(@RequestParam String message) {
		kafkaProducerService.sendMessage(message);
		return "Message sent: " + message;
	}
//	curl "http://localhost:8080/api/kafka/publish?message=HelloKafka"

}
