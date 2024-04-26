package com.ashfaq.dev.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ashfaq.dev.services.KafkaProducerService;

@RestController
public class MainController {

	@Value("${springcustom.topic.name}")
	private String topicName;

	@Autowired
	private KafkaProducerService kafkaProducerService;

	@GetMapping("/Senddata")
	public String sendmessage() {

		List.of(1, 2, 3, 4, 5, 6).toString();

		kafkaProducerService.sendMessage(topicName, "Sending Data : " + List.of(1, 2, 3, 4, 5, 6).toString());

		return "Send complete";
	}

	@GetMapping("/Senddata2")
	public String sendmessageOPT2() {

		List.of(1, 2, 3, 4, 5, 6).toString();

		kafkaProducerService.sendMessage(topicName, "Sending Data : " + List.of(1, 2, 3, 4, 5, 6).toString());

		return "Send complete";
	}

}
