package com.ashfaq.dev.Config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
public class KafkaConsumerConfig {


	@Value("${spring.kafka.consumer.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${spring.kafka.consumer.key-deserializer}")
	private String keySerializer;

	@Value("${spring.kafka.consumer.value-deserializer}")
	private String valueSerializer;

	@Value("${spring.kafka.consumer.group-id}")
	private String groupid;

//	    @Value("${kafka.security.protocol}")
//	    private String securityProtocol;
//
//	    @Value("${kafka.sasl.mechanism}")
//	    private String saslMechanism;
//
//	    @Value("${kafka.sasl.jaas.config}")
//	    private String saslJaasConfig;

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
		configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keySerializer);
		configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueSerializer);

//	    configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
//	    configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000);
//	    configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
		
//      configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "kafka.security.protocol=SASL_SSL");
//      configProps.put(SaslConfigs.SASL_MECHANISM, "kafka.sasl.mechanism=PLAIN");
//      configProps.put(SaslConfigs.SASL_JAAS_CONFIG, "kafka.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='your_username' password='your_password';

		return new DefaultKafkaConsumerFactory<>(configProps);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}
}
