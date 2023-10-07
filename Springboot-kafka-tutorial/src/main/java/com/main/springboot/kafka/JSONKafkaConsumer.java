/**
 * Code developed by Ashfaq (© [Year])
 * GitHub: https://github.com/DarkSharkAsh
 */

package com.main.springboot.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.main.springboot.model.User;

@Service
public class JSONKafkaConsumer {

	// default logging
	private static final Logger LOGGER = LoggerFactory.getLogger(JSONKafkaConsumer.class);

	@KafkaListener(topics = "myCustomTopic1JSON",groupId = "myGroup")
	public void conusmer_JSON(User message) {
		LOGGER.info(String.format(JSONKafkaConsumer.class.toGenericString()+" ConsumerJSON(); JSON message received -> %s", message));
	}

}
