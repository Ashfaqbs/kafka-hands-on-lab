package com.ashfaq.dev.services;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

//	@Value("${springcustom.topic.name}")
//	private String topicName;
	
    @KafkaListener(topics = "${springcustom.topic.name}", groupId = "group_id")
    public void listen(String message) {
        System.out.println("Received Message: " + message);
    }
}
