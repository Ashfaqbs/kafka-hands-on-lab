/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */

package com.main.springboot.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {
//producer class annotated with @Service

	// default logger
	private static Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

//to send message to topic we are using kafkaTemplate as spring will provide autoconfiguration to it , we need to
//	inject and use it
	private KafkaTemplate<String, String> kafkaTemplate;

	public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void sendMessage(String message) {
		// We will create method which will use a kafaka template to send mesage to
		// topic
		logger.info(String.format("Message sent %s", message));
		
		kafkaTemplate.send("myCustomTopic1", message);

	}

}
