package com.example.kafkareference.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics as Spring beans.
 *
 * @Configuration — marks this class as a source of @Bean definitions.
 *   Spring scans for this and calls each @Bean method at startup.
 *
 * How topic creation works:
 *   Spring Kafka auto-registers a KafkaAdmin bean (from bootstrap-servers config).
 *   KafkaAdmin detects all NewTopic beans in the context and creates any that are missing.
 *   If the topic already exists (e.g. created by kafka-setup), this is a no-op.
 *   This acts as a safety net for running outside Docker (e.g. CI, tests).
 *
 * Replication factor 3:
 *   Each partition has 3 copies, one on each broker.
 *   The cluster can lose 1 broker and keep serving reads and writes.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topics.raw-orders}")
    private String rawOrdersTopic;

    @Value("${kafka.topics.enriched-orders}")
    private String enrichedOrdersTopic;

    @Value("${kafka.topics.dead-letter}")
    private String deadLetterTopic;

    @Bean
    public NewTopic rawOrdersTopic() {
        return TopicBuilder.name(rawOrdersTopic)
            .partitions(3)
            .replicas(3)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(24 * 60 * 60 * 1000L))
            .build();
    }

    @Bean
    public NewTopic enrichedOrdersTopic() {
        return TopicBuilder.name(enrichedOrdersTopic)
            .partitions(3)
            .replicas(3)
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(24 * 60 * 60 * 1000L))
            .build();
    }

    @Bean
    public NewTopic deadLetterTopic() {
        return TopicBuilder.name(deadLetterTopic)
            .partitions(1)
            .replicas(3)
            // Keep DLT messages for 7 days — longer than normal topics for investigation
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7 * 24 * 60 * 60 * 1000L))
            .build();
    }
}
