package com.example.kafka_consumer_sb3.config;

import com.example.kafka_producer_sb3.dto.EmployeeDTO;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;
@Configuration
public class KafkaConsumerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
// Default Configuration
//    @Bean
//    public ConsumerFactory<String, EmployeeDTO> consumerFactory() {
//        Map<String, Object> config = new HashMap<>();
//        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        config.put(ConsumerConfig.GROUP_ID_CONFIG, "employee-group");
//        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
//        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
//        return new DefaultKafkaConsumerFactory<>(config);
//    }
//
//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, EmployeeDTO> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, EmployeeDTO> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        return factory;
//    }

// High Throughput Configuration
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "employee-group");

        // High Throughput Settings
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 200); // Fetch 200 records at a time
        configProps.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 64 * 1024); // 64KB Minimum fetch
        configProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50); // Wait 50ms for batch
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000); // 30 Sec timeout

        return new DefaultKafkaConsumerFactory<>(configProps);
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true); // Enable batch processing
        return factory;
    }

    @Bean
    public NewTopic createTopic() {
        return TopicBuilder.name("employee-topic").partitions(3).replicas(1).build();
    }
}
