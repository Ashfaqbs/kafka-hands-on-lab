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

	// api
	/*
	 * curl http://localhost:8080/api/kafka/publish?message=Message1 curl
	 * http://localhost:8080/api/kafka/publish?message=Message2 curl
	 * http://localhost:8080/api/kafka/publish?message=Message3 curl
	 * http://localhost:8080/api/kafka/publish?message=Message4
	 * 
	 * 
	 */

	@GetMapping("/publishWithKey")
	public String publishMessageWithKey(@RequestParam String key, @RequestParam String message) {
		kafkaProducerService.sendMessageWithKey(key, message);
		return "Message sent with key: " + key + ", message: " + message;
	}

	// api
	/*
	 * curl
	 * "http://localhost:8080/api/kafka/publishWithKey?key=key1&message=HelloPartition1"
	 * curl
	 * "http://localhost:8080/api/kafka/publishWithKey?key=key2&message=HelloPartition2"
	 * curl
	 * "http://localhost:8080/api/kafka/publishWithKey?key=key1&message=AnotherMessageForPartition1"
	 * 
	 */
}
