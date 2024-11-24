package com.example.kafka.mainservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class MainServiceProducer {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private MessageRepository messageRepository;

	@PostMapping("/publish")
	public String sendMessage(@RequestParam String messageContent) {
		Message message = new Message();
		message.setContent(messageContent);
		message.setAckFromDs1(null);
		message.setAckFromDs2(null);

		messageRepository.save(message);
		kafkaTemplate.send("main-topic", messageContent);
		return "Message sent: " + messageContent;
	}
}
