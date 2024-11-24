package com.example.kafka.mainservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MainServiceConsumer {

	@Autowired
	private MessageRepository messageRepository;

	@KafkaListener(topics = "ack-topic", groupId = "main-group")
	public void consumeAck(String ackMessage) {
		System.out.println("Ack message received: " + ackMessage);

		String[] parts = ackMessage.split(":");
		String source = parts[0]; // e.g., "ds1-ack" or "ds2-ack"
		String ackStatus = parts[1]; // e.g., "Success for message - HelloKafka"

		String originalMessageContent = ackStatus.split(" - ")[1]; // Extract the message

		Message message = messageRepository.findByContent(originalMessageContent);
		if (message != null) {
			if ("ds1-ack".equals(source)) {
				message.setAckFromDs1("Success");
			} else if ("ds2-ack".equals(source)) {
				message.setAckFromDs2("Success");
			}
			messageRepository.save(message); // Update the database
		}
	}
}
