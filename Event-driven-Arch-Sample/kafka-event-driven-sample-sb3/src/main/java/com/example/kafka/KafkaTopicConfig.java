package com.example.kafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

//	@Bean
//	public KafkaAdmin kafkaAdmin() {
//		Map<String, Object> configs = new HashMap<>();
//		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		return new KafkaAdmin(configs);
//	}

	@Bean
	public NewTopic mainTopic() {
		return new NewTopic("main-topic", 3, (short) 1); // 3 partitions, replication factor 1
	}

	@Bean
	public NewTopic ackTopic() {
		return new NewTopic("ack-topic", 3, (short) 1); // 3 partitions, replication factor 1
	}
}
