package com.ashfaq.dev.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
	
	
	public void sendMessage(String topic, int partition, String key, String value) {
	    ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, key, value);
	    kafkaProducer.send(record);
	    kafkaProducer.flush();
	}

	
	
	
}
