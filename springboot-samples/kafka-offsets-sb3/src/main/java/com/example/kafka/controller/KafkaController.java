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

	@Autowired
	private KafkaProducerService kafkaProducerService;

	@GetMapping("/publish")
	public String publishMessage(@RequestParam("message") String message) {
		kafkaProducerService.sendMessage(message);
		return "Message sent: " + message;
	}
}
