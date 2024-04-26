package com.ashfaq.dev.Config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class KafkaProducerConfig {

	@Value("${spring.kafka.producer.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${spring.kafka.producer.key-serializer}")
	private String keySerializer;

	@Value("${spring.kafka.producer.value-serializer}")
	private String valueSerializer;

//	    @Value("${kafka.security.protocol}")
//	    private String securityProtocol;
//
//	    @Value("${kafka.sasl.mechanism}")
//	    private String saslMechanism;
//
//	    @Value("${kafka.sasl.jaas.config}")
//	    private String saslJaasConfig;

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

//        configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "kafka.security.protocol=SASL_SSL");
//        configProps.put(SaslConfigs.SASL_MECHANISM, "kafka.sasl.mechanism=PLAIN");
//        configProps.put(SaslConfigs.SASL_JAAS_CONFIG, "kafka.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='your_username' password='your_password';
//);

		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
}
