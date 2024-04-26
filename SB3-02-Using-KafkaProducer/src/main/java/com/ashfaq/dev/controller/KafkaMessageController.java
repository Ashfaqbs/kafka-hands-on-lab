package com.ashfaq.dev.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ashfaq.dev.service.KafkaConsumerService;
import com.ashfaq.dev.service.KafkaSenderService;

@RestController
public class KafkaMessageController {
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageController.class);

	@Value("${springcustom.topic.name}")
	private String topicName;
	@Autowired
	private KafkaSenderService kafkaSenderService;

	@GetMapping("/send")
	public String sendMessageToKafkaTopic() {
		LOGGER.info("Entering the sending service  ");
		String message = "Sample data : ";
		LOGGER.info("Sending  the data : " + message);
		kafkaSenderService.sendMessage(topicName, message);
		LOGGER.info("Sending  the data : " + message + " is completed ");
		return "Message sent successfully";
	}
}
