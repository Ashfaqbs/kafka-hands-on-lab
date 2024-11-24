package com.example.kafka.listeners;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {

	@KafkaListener(topics = "my-topic", groupId = "group1")
	public void listenerGroup1(ConsumerRecord<String, String> record) {
		System.out.println("group1 Received message: " + record.value());
		System.out.println("group1 Topic: " + record.topic());
		System.out.println("group1 Partition: " + record.partition());
		System.out.println("group1 Offset: " + record.offset());
	}

	@KafkaListener(topics = "my-topic", groupId = "group2")
	public void listenerGroup2(ConsumerRecord<String, String> record) {
		System.out.println("group2 Received message: " + record.value());
		System.out.println("group2 Topic: " + record.topic());
		System.out.println("group2 Partition: " + record.partition());
		System.out.println("group2 Offset: " + record.offset());
	}
}
