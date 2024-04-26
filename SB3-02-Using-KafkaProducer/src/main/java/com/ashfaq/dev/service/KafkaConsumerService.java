package com.ashfaq.dev.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service

public class KafkaConsumerService {

//	@Value("${springcustom.topic.name}")
//	private String topicName;

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);

	@KafkaListener(topics = "${springcustom.topic.name}", groupId = "group_id")
	public void listen(String message) {
		LOGGER.info("Entering the consumer ");
		LOGGER.info("Received Message: " + message);
		LOGGER.info("Exiting the consumer");
	}
}