package com.example.order_service.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import com.example.base_domains.dto.OrderEvent;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class OrderProducer {

	@Autowired
	private NewTopic topic;
	@Autowired
	private KafkaTemplate<String, OrderEvent> kafkaTemplate;

	public void sendMessage(OrderEvent orderEvent) {
		log.info(String.format("OrderEvent sent -> %s", orderEvent));

//		1ST WAY SEND MESSAGE WITH TOPIC NAME
//		kafkaTemplate.send(topic.name(), orderEvent);

//		2ND WAY SEND MESSAGE WITH MESSAGE OBJECT AS PARAMETER
		// Send Message
		Message<OrderEvent> message = MessageBuilder.withPayload(orderEvent).setHeader(KafkaHeaders.TOPIC, topic.name())
				.build();

		kafkaTemplate.send(message);

	}

}
