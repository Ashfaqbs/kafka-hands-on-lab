package com.ashfaq.dev.service;

import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaSenderService {

	@Autowired
	private KafkaProducer<String, String> kafkaProducer;

	public void sendMessage(String topic, String message) {
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
		
//		Future<RecordMetadata> send = kafkaProducer.send(record);

		kafkaProducer.send(record, (metadata, exception) -> {
			if (exception != null) {
				exception.printStackTrace();
			} else {
				System.out.println("Sent message: " + message + " with offset: " + metadata.offset());
			}
		});
		kafkaProducer.flush();
	}
}
