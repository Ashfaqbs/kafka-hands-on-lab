package com.example.kafkareference.config;

import com.example.kafkareference.dto.RawOrderEvent;
import com.example.kafkareference.exception.NonRetryableProcessingException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer configuration — the most important config class in this project.
 *
 * Key production decisions made here:
 *
 * 1. MANUAL ACK (AckMode.MANUAL_IMMEDIATE)
 *    Offset is committed only when the listener explicitly calls ack.acknowledge().
 *    If the app crashes before ACK, the message is re-delivered — at-least-once guarantee.
 *    Never lose a message even during restarts or failures.
 *
 * 2. DefaultErrorHandler with ExponentialBackOff (3 retries)
 *    Retry 1 → wait 1s
 *    Retry 2 → wait 2s
 *    Retry 3 → wait 4s
 *    After 3 failures → send to DLT and commit offset (move on)
 *
 * 3. DeadLetterPublishingRecoverer
 *    Messages that fail all retries land in orders.raw.DLT with headers containing
 *    the original topic, partition, offset, and exception details — fully traceable.
 *
 * 4. NonRetryableProcessingException → skip retries, go straight to DLT
 *    Bad data (invalid schema, business rule violation) will never succeed on retry.
 *    Skipping retries saves time and reduces noise in logs.
 *
 * 5. ErrorHandlingDeserializer wraps JsonDeserializer
 *    If a message cannot be deserialized (malformed JSON), it goes to DLT
 *    instead of crashing the consumer or getting stuck in an infinite retry loop.
 *
 * 6. Concurrency = 3
 *    3 consumer threads, matching the 3 partitions on orders.raw.
 *    Each thread owns one partition — parallel processing, in-order within a partition.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.properties.security.protocol}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.sasl.jaas.config}")
    private String saslJaasConfig;

    @Value("${kafka.topics.dead-letter}")
    private String deadLetterTopic;

    @Value("${kafka.consumer.concurrency}")
    private int concurrency;

    @Value("${kafka.consumer.retry.max-attempts}")
    private int maxAttempts;

    @Value("${kafka.consumer.retry.initial-interval-ms}")
    private long initialIntervalMs;

    @Value("${kafka.consumer.retry.multiplier}")
    private double multiplier;

    @Value("${kafka.consumer.retry.max-interval-ms}")
    private long maxIntervalMs;

    @Bean
    public ConsumerFactory<String, RawOrderEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // MANUAL ACK — never auto-commit offsets
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // SASL/SCRAM-SHA-512
        props.put("security.protocol", securityProtocol);
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.jaas.config", saslJaasConfig);

        // Wraps JsonDeserializer: deserialization errors go to DLT instead of crashing
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());

        // Deserializer settings
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.kafkareference.dto");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, RawOrderEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        // Tuning: how many records to fetch per poll (default 500)
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        // How long the broker waits before declaring this consumer dead (default 300s)
        // Lower = faster rebalance on crash, but increase with slow processing
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // ── Dead letter recoverer ────────────────────────────────────────────
        // All failed messages (after retries exhausted OR NonRetryable) land in orders.raw.DLT.
        // DLT messages carry these headers automatically:
        //   kafka_dlt-original-topic, kafka_dlt-original-partition,
        //   kafka_dlt-original-offset, kafka_dlt-exception-message, kafka_dlt-exception-stacktrace
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, ex) -> {
                log.error("Sending to DLT after all retries. topic={} partition={} offset={} error={}",
                    record.topic(), record.partition(), record.offset(), ex.getMessage());
                return new TopicPartition(deadLetterTopic, 0);
            }
        );

        // ── Exponential backoff ──────────────────────────────────────────────
        // attempt 1: wait 1s, attempt 2: wait 2s, attempt 3: wait 4s → DLT
        ExponentialBackOff backOff = new ExponentialBackOff(initialIntervalMs, multiplier);
        backOff.setMaxInterval(maxIntervalMs);
        backOff.setMaxElapsedTime((long) (initialIntervalMs * Math.pow(multiplier, maxAttempts)));

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // ── Skip retries for permanent failures ──────────────────────────────
        // These exceptions go straight to DLT — retrying will not help.
        handler.addNotRetryableExceptions(NonRetryableProcessingException.class);
        handler.addNotRetryableExceptions(org.springframework.kafka.support.serializer.DeserializationException.class);

        // Log the exception at each retry so you can see it in the logs
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
            log.warn("Retry attempt {} for topic={} partition={} offset={} error={}",
                deliveryAttempt, record.topic(), record.partition(), record.offset(), ex.getMessage())
        );

        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RawOrderEvent> kafkaListenerContainerFactory(
        ConsumerFactory<String, RawOrderEvent> consumerFactory,
        DefaultErrorHandler errorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, RawOrderEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // MANUAL_IMMEDIATE: ack.acknowledge() in the listener commits the offset immediately.
        // The alternative MANUAL batches ACKs — MANUAL_IMMEDIATE is simpler and safer.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Wire in the error handler with retries + DLT
        factory.setCommonErrorHandler(errorHandler);

        // 3 concurrent threads = one per partition (matches topic partition count)
        factory.setConcurrency(concurrency);

        // Graceful shutdown: let in-flight messages complete before stopping
        factory.getContainerProperties().setShutdownTimeout(30_000L);

        return factory;
    }
}
