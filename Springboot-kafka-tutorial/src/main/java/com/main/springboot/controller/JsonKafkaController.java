/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */

package com.main.springboot.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.main.springboot.kafka.KafkaJsonProducer;
import com.main.springboot.model.User;

@RestController
//@RequestMapping("/api/v1/jsonKafka")
@RequestMapping("api/v1/kafka")
public class JsonKafkaController {

	private KafkaJsonProducer jsonProducer;

	public JsonKafkaController(KafkaJsonProducer jsonProducer) {
		this.jsonProducer = jsonProducer;
	}

	@PostMapping("/publish")
	public ResponseEntity<String> publishJsonMessage(@RequestBody User user) {
		jsonProducer.sendJsonMessage(user);

		return ResponseEntity.ok("JSON Message Sent to kafka topic");
	}
}
