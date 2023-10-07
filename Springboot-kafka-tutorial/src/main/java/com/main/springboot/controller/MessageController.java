/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */

package com.main.springboot.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.main.springboot.kafka.KafkaProducer;

@RestController
@RequestMapping("api/v1/kafka")
public class MessageController {

	private KafkaProducer kafkaProducer;

	// this time we will use , depecdency injection via constructor
//	@Autowired this annotation we no need to add , if the constructor  has only one argument
	public MessageController(KafkaProducer kafkaProducer) {
		this.kafkaProducer = kafkaProducer;
	}

	// http:localhost:8080/api/v1/kafka/publish?message=this is a sample message
	@GetMapping("/publish")
	public ResponseEntity<String> publishMessage(@RequestParam("message") String message) {

		kafkaProducer.sendMessage(message);

		return ResponseEntity.ok("Message sent");
	}

}
