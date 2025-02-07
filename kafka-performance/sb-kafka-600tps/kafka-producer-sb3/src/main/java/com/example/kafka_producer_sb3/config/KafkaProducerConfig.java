package com.example.kafka_producer_sb3.config;

import com.example.kafka_producer_sb3.dto.EmployeeDTO;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

//    Default Configuration
//    @Bean
//    public ProducerFactory<String, EmployeeDTO> producerFactory() {
//        Map<String, Object> config = new HashMap<>();
//        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//        return new DefaultKafkaProducerFactory<>(config);
//    }

//    High Throughput Configuration
    @Bean
    public ProducerFactory<String, EmployeeDTO> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // High Throughput Settings
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_768); // 32KB batch size
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 10); // Wait 10ms before sending batch
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Fast Compression
        configProps.put(ProducerConfig.ACKS_CONFIG, "1"); // Leader Acknowledgment
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB Buffer

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, EmployeeDTO> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public NewTopic createTopic() {
        return TopicBuilder.name("employee-topic").partitions(3).replicas(1).build();
    }
}
