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
		// Set the consumer to start from the earliest offset
                configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


		configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
		configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000);
		configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);


//      configProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL"); for consumer this is fine , as AdminClientConfig is for more acess 
// configProps.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL
		
 //       Admin Client Configurations: These are specifically designed for administrative tasks, allowing for operations such as creating, deleting, 
// 	and managing topics, consumers, and producers within a Kafka cluster. The admin client configurations are focused on administrative
// 	tasks and include settings for SSL/TLS, authentication, and other security-related parameters necessary for administrative operations 4.
 //        Common Client Configurations: These are broader configurations applicable to both producers and consumers, including settings for connection details, 
	// serialization/deserialization, and consumer-specific settings like group ID, auto-offset-reset, and session timeout. While these configurations are
	// not exclusive to consumers, they are essential for defining how a consumer interacts with the Kafka cluster, including how it reads data, handles offsets,
	// and manages its connection to the cluster 2.
		
		
//      configProps.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
//      configProps.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username='your_username' password='your_password';");


		

		return new DefaultKafkaConsumerFactory<>(configProps);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}
}
