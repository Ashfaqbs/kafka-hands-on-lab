package com.ashfaq.dev.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
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

	
	
	
//	PS C:\kafka> .\bin\windows\kafka-topics.bat  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 9 --topic mytutTopic

	@KafkaListener(topicPartitions = @TopicPartition(topic = "mytutTopic", partitions = { "2", "1" }))
	public void listenToPartitionTwo(ConsumerRecord<String, String> record) {
//if we are sending the data to  partition 2: they we have to listen to that partition partitions = { "0" ,"1"}
		// other wise we cant listen to it , so partitions = { "2" }, and for multiple
		// partitions = { "2", "1" }
		System.out.println("Received message from partition 2:");
		System.out.println("Key: " + record.key());
		System.out.println("Value: " + record.value());
		System.out.println("--------------------------");
	}

	// OP
//	Received message from partition 2:
//		Key: order123
//		Value: {"orderId": "order123", "items": [{"productId": "p1", "quantity": 2}, {"productId": "p2", "quantity": 1}], "customer": {"name": "John Doe", "email": "john@example.com"}}

}