package com.example.kafkareference.producer;

import com.example.kafkareference.dto.EnrichedOrderEvent;
import com.example.kafkareference.exception.RetryableProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Sends the enriched order event to orders.enriched after successful DB save.
 *
 * @Component — registers this class as a Spring-managed bean.
 *   Like @Service but communicates "infrastructure/integration" rather than
 *   "business logic." Both are functionally identical singleton beans.
 *   Spring injects an instance of this wherever EnrichedOrderProducer is needed.
 *
 * ─── PRODUCER (BROKER) ACK — how it works ───────────────────────────────────
 *
 *   KafkaTemplate.send() is asynchronous — it returns a Future immediately.
 *   Calling .get(timeout) on that Future BLOCKS until the broker responds.
 *
 *   What the broker confirms when it ACKs (acks=all in KafkaProducerConfig):
 *     All in-sync replicas (ISR) have written the message to their local logs.
 *     With 3 replicas, all 3 brokers have the message persisted.
 *     Even if the leader broker crashes immediately after, the message is not lost —
 *     one of the 2 replicas becomes the new leader and the message is there.
 *
 *   The SendResult contains:
 *     - topic     = "orders.enriched"
 *     - partition = which partition the message landed on (determined by key hash)
 *     - offset    = the position of this message in that partition's log
 *
 *   This is a DIFFERENT kind of ACK from the consumer's ack.acknowledge():
 *     Consumer ACK  → "I finished consuming message X from orders.raw"
 *     Producer ACK  → "Kafka confirmed message Y is durably stored in orders.enriched"
 *
 * ─── Why synchronous send? ───────────────────────────────────────────────────
 *
 *   Synchronous: the caller (OrderProcessingService) knows the send succeeded
 *   before returning. If send fails, a RetryableException is thrown, the
 *   @Transactional rolls back the DB save, and the error handler retries cleanly.
 *
 *   Async alternative: fire-and-forget with a callback. Higher throughput but
 *   the DB commit and Kafka send can diverge on failure — DB row saved but
 *   the enriched event never reached Kafka. Harder to detect and recover from.
 *
 *   For this reference project: synchronous is the correct choice. The latency
 *   cost (~5–20ms per message) is acceptable given the consistency benefit.
 */
// @Component — Spring-managed singleton. KafkaTemplate and topic value are injected.
@Component
public class EnrichedOrderProducer {

    private static final Logger log = LoggerFactory.getLogger(EnrichedOrderProducer.class);
    private static final int SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    // @Value("${kafka.topics.enriched-orders}") — injects the topic name from
    // application.yml at construction time. Avoids hardcoding topic names in code,
    // making it easy to change topics per environment via config.
    private final String topic;

    public EnrichedOrderProducer(
        KafkaTemplate<String, Object> kafkaTemplate,
        @Value("${kafka.topics.enriched-orders}") String topic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    /**
     * Sends to orders.enriched and blocks until the broker ACKs (acks=all).
     *
     * Partition key: orderId.toString()
     *   Kafka hashes the key to determine which partition receives the message.
     *   Using orderId as the key means: all events for the same order always land
     *   on the same partition → ordered delivery per order.
     *
     * @throws RetryableProcessingException if broker does not ACK within 10 seconds.
     *   The error handler will retry the whole Kafka message (including DB save).
     *   Idempotency check in OrderProcessingService prevents duplicate DB rows on retry.
     */
    public void send(EnrichedOrderEvent event) {
        try {
            // kafkaTemplate.send(topic, key, value)
            //   key   = orderId → determines partition (consistent routing per order)
            //   value = enriched event → serialized to JSON by JsonSerializer
            //
            // .get(10, TimeUnit.SECONDS) — BLOCKS here until:
            //   SUCCESS: broker confirms all ISR replicas wrote the message
            //            → returns SendResult with topic/partition/offset metadata
            //   TIMEOUT: broker didn't respond within 10 seconds → TimeoutException
            //   FAILURE: broker rejected or broker unreachable → ExecutionException
            //
            // ── BROKER ACK received here ─────────────────────────────────────
            SendResult<String, Object> result = kafkaTemplate
                .send(topic, event.orderId().toString(), event)
                .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            // Log the broker's confirmed location — topic, partition, and offset.
            // This lets you trace the downstream message: e.g. check orders.enriched
            // partition 1 offset 42 to find the exact message produced here.
            log.debug("Broker ACK received: orderId={} → topic={} partition={} offset={}",
                event.orderId(),
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());

        } catch (TimeoutException e) {
            // Broker is alive but slow — could be a replication lag or network blip.
            // Retryable: worth trying again after backoff.
            throw new RetryableProcessingException(
                "Kafka send timed out for orderId=" + event.orderId(), e);

        } catch (ExecutionException e) {
            // Broker rejected the message or is unreachable.
            // e.getCause() has the real reason (e.g. NotLeaderOrFollowerException, AuthException).
            // Retryable: leader election in progress, broker restart, etc.
            throw new RetryableProcessingException(
                "Kafka send failed for orderId=" + event.orderId(), e.getCause());

        } catch (InterruptedException e) {
            // Thread was interrupted while waiting for broker ACK.
            // Restore the interrupted flag so the JVM can handle shutdown cleanly.
            Thread.currentThread().interrupt();
            throw new RetryableProcessingException(
                "Kafka send interrupted for orderId=" + event.orderId(), e);
        }
    }
}
