package com.example.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

	private static final String TOPIC_NAME = "my-topic";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	// here we are not providing the key , i,e how the data is sent to which
	// partition of the topic is decided by kafka itself ,
//	 and Kafka has its own algo like round-robin , hash , random etc. this exlaination can be seen in md files of this repo and offset repo.
	public void sendMessage(String message) {
		kafkaTemplate.send(TOPIC_NAME, message);
	}

//	here we are  providing the key, 
//	providing a key allows you to control which partition the data is sent to! 
//	When a producer sends a message with a key, Kafka uses a hashing algorithm to determine the target partition based on the key
// like  working will be like partition = hash(key) % number_of_partitions

	public void sendMessageWithKey(String key, String message) {
		kafkaTemplate.send("my-topic", key, message);
	}
}
