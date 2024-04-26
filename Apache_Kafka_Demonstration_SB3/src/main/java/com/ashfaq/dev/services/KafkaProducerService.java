package com.ashfaq.dev.services;

import java.sql.SQLException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

	public void sendMessage(String topic, String message) {
		kafkaTemplate.send(topic, message);
	}

	public void sendMessage2(String topic, String message) {
		kafkaTemplate.send(topic, message);
	}

	
}
