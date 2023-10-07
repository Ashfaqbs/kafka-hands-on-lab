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
public class KafkaConsumer {

	private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

	// we are creating a method which will subscribe to our topic to get the
	// messages from topic ,
	@KafkaListener(topics = "myCustomTopic1", groupId = "myGroup") // this annotation will help to listen or subscribe
																	// to the topic ,
	public void conusmer(String message) {
		logger.info(String.format("message received -> %s", message));
	}
//now if we run the application and call the api and send messages all the messages will be collected and printed in the console by this consumer 
	// function

	// FOR JSON new Topic

	@KafkaListener(topics = "myCustomTopic1JSON", groupId = "myGroup") // this annotation will help to listen or subscribe
	// to the topic ,
	public void conusmer_JSON(User message) {
		logger.info(String.format("JSON message received -> %s", message));
	}
}
