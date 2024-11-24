package com.ashfaq.dev.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

//	PS C:\kafka> .\bin\windows\kafka-topics.bat  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 9 --topic mytutTopic

	@GetMapping("/sendv2")
	public String sendMessage() {
		// Hardcoded key and value
		String key = "order123";
		String value = "{\"orderId\": \"order123\", \"items\": [{\"productId\": \"p1\", \"quantity\": 2}, {\"productId\": \"p2\", \"quantity\": 1}], \"customer\": {\"name\": \"John Doe\", \"email\": \"john@example.com\"}}";

		kafkaSenderService.sendMessage("mytutTopic", 2, key, value);
		return "Message sent to topic: " + "mytutTopic" + ", partition: " + 2;
	}

}
