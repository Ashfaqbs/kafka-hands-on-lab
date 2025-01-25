package com.example.sb_kafka_docker_demo.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import com.example.sb_kafka_docker_demo.dto.MyMessage;
@Service
public class KafkaProducerService {

    @Autowired
    // private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaTemplate<String, MyMessage> kafkaTemplate;

    public void sendMessage() {
        MyMessage message = new MyMessage("testKey", "testValue");
        Message<MyMessage> message1 = MessageBuilder.withPayload(message).setHeader(KafkaHeaders.TOPIC, "my-topic")
				.build();
        kafkaTemplate.send("my-topic", message);
    }


    
   
}

