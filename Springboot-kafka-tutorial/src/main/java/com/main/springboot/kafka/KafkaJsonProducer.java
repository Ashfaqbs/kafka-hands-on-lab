/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */

package com.main.springboot.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import com.main.springboot.model.User;

@Service
public class KafkaJsonProducer {

	private static Logger logger = LoggerFactory.getLogger(KafkaJsonProducer.class);

	private KafkaTemplate<String, User> kafkaTemplate;

	// constructor based dependecy injection
	public KafkaJsonProducer(KafkaTemplate<String, User> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void sendJsonMessage(User userData) {
		logger.info(String.format("JSON Message sent -> %s",userData.toString()));
		Message<User> message = MessageBuilder.withPayload(userData).setHeader(KafkaHeaders.TOPIC, "myCustomTopic1JSON")
				.build();

		this.kafkaTemplate.send(message);

	}

}
