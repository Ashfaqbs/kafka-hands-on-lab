package com.example.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerListener {

	@KafkaListener(topics = "my-topic", groupId = "my-group")
	public void consumeMessage(ConsumerRecord<String, String> record) {
		System.out.println("Received message: " + record.value());
		System.out.println("Topic: " + record.topic());
		System.out.println("Partition: " + record.partition());
		System.out.println("Offset: " + record.offset());
	}
}
