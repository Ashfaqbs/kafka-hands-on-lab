package com.example.kafkareference.consumer;

import com.example.kafkareference.dto.RawOrderEvent;
import com.example.kafkareference.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka listener — entry point for all messages arriving on orders.raw.
 *
 * ═══════════════════════════════════════════════════════════════════════════
 *                          FULL MESSAGE FLOW
 * ═══════════════════════════════════════════════════════════════════════════
 *
 *  Python Producer (SASL/SCRAM-SHA-512, acks=all, idempotent)
 *       │
 *       ▼
 *  [orders.raw] ── 3 partitions, 3 replicas (1 per broker)
 *       │
 *       │  Spring Kafka polls the topic and calls consume() below.
 *       │  3 concurrent threads (one per partition) run in parallel.
 *       │
 *       ▼
 *  OrderConsumer.consume()
 *       │
 *       ├─ delegates ALL business logic to OrderProcessingService
 *       │         │
 *       │         ├─ 1. existsByOrderId?  ──→ skip (duplicate guard)
 *       │         ├─ 2. validate()        ──→ NonRetryableException → straight to DLT
 *       │         ├─ 3. compute totalAmount
 *       │         ├─ 4. repository.save() ──→ PostgreSQL   ✓ PERSISTED IN DB
 *       │         │         └─ DataAccessException → RetryableException → retry loop
 *       │         └─ 5. producer.send()   ──→ orders.enriched (blocks until broker ACK)
 *       │                                                   ✓ BROKER ACK CONFIRMED
 *       │                                                     (acks=all → all ISR replicas)
 *       │
 *       ├─ SUCCESS path:
 *       │       ack.acknowledge()   ← CONSUMER ACK: commits offset to __consumer_offsets
 *       │                             = "I'm done with this message, don't redeliver it"
 *       │                             Persisted in Kafka's internal __consumer_offsets topic.
 *       │                             NOT in PostgreSQL.
 *       │
 *       └─ FAILURE path (any exception thrown from process()):
 *               ack.acknowledge() is NEVER called
 *                       │
 *                       ▼
 *               DefaultErrorHandler
 *                       ├─ NonRetryable? → DLT immediately (no backoff)
 *                       ├─ Retryable?    → backoff 1s → 2s → 4s → redeliver
 *                       └─ Exhausted?    → DLT → then commits offset (moves on)
 *                               │
 *                               ▼
 *               [orders.raw.DLT] ── headers: original topic, partition, offset, exception
 *
 * ═══════════════════════════════════════════════════════════════════════════
 *  TWO KINDS OF ACK — understanding this is critical:
 *
 *  1. CONSUMER ACK  — ack.acknowledge() in this file
 *       What: commits the offset to Kafka's __consumer_offsets topic
 *       Meaning: "I successfully processed offset N on partition P"
 *       Effect: if the app restarts, it resumes from N+1 (not from N again)
 *       Stored in: Kafka (__consumer_offsets topic), NOT in PostgreSQL
 *
 *  2. PRODUCER ACK  — .get(10s) in EnrichedOrderProducer
 *       What: blocks until the orders.enriched broker confirms receipt
 *       Meaning: "my enriched message is durably written to all ISR replicas"
 *       Effect: downstream consumers of orders.enriched will see this message
 *       Stored in: Kafka (orders.enriched topic log), NOT in PostgreSQL
 *
 *  WHAT IS IN POSTGRESQL:
 *       Only the ProcessedOrder row (step 4 above).
 *       It contains the full order + computed totalAmount + kafka metadata
 *       (topic, partition, offset) so you can trace any DB row back to its
 *       exact Kafka message for debugging or replay.
 *
 *  ORDER MATTERS: DB save happens BEFORE Kafka send.
 *       Why: DB is the source of truth. If Kafka send fails after DB save,
 *       the data is safe in the DB and the error handler retries the Kafka send.
 *       If we sent to Kafka first and DB save failed, downstream would have
 *       data that we don't — a dangerous inconsistency.
 * ═══════════════════════════════════════════════════════════════════════════
 *
 *  THIS CLASS ONLY:
 *   - Receives the raw message with metadata
 *   - Delegates to the service
 *   - ACKs on success
 *   - Lets exceptions propagate (never catch here — the error handler needs to see them)
 */
// @Component — registers this class as a Spring-managed bean.
// Spring creates one instance at startup and injects it wherever needed.
// @KafkaListener requires the containing class to be a Spring bean.
@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderProcessingService processingService;

    // Constructor injection — Spring sees one constructor and injects automatically.
    // No @Autowired needed on a single-constructor bean (Spring 4.3+).
    public OrderConsumer(OrderProcessingService processingService) {
        this.processingService = processingService;
    }

    // @KafkaListener — marks this method as a Kafka message handler.
    //
    //   topics: reads the topic name from application.yml (kafka.topics.raw-orders).
    //     Using ${...} instead of a hardcoded string lets you change topics per environment
    //     without recompiling.
    //
    //   groupId: all instances of this app with the same group share the partitions.
    //     If you run 3 instances, each gets one partition (partition count = concurrency).
    //     If you run 1 instance, it owns all 3 partitions.
    //
    //   containerFactory: refers to the kafkaListenerContainerFactory bean defined in
    //     KafkaConsumerConfig. This wires in MANUAL_IMMEDIATE ACK, error handler,
    //     concurrency=3, and SASL credentials.
    @KafkaListener(
        topics = "${kafka.topics.raw-orders}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
        // @Payload — tells Spring Kafka to deserialize the message value into RawOrderEvent.
        // Deserialization uses ErrorHandlingDeserializer → JsonDeserializer (configured in
        // KafkaConsumerConfig). If deserialization fails, the error lands in the DLT —
        // this method is never even called for a poison pill message.
        @Payload RawOrderEvent event,

        // @Header(KafkaHeaders.RECEIVED_TOPIC) — extracts the topic name from the
        // Kafka message headers. Useful for logging and passing to the service for
        // audit storage in the DB row.
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,

        // @Header(KafkaHeaders.RECEIVED_PARTITION) — the partition number (0, 1, or 2).
        // Each thread in the concurrent listener owns one partition, so this tells you
        // which thread/partition handled this message.
        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,

        // @Header(KafkaHeaders.OFFSET) — the offset of this message within its partition.
        // Together with topic + partition, this uniquely identifies any Kafka message.
        // Stored in the DB row so you can replay exact messages if needed.
        @Header(KafkaHeaders.OFFSET) long offset,

        // Acknowledgment — Spring Kafka injects this when AckMode is MANUAL or MANUAL_IMMEDIATE.
        // Calling ack.acknowledge() commits the current offset to Kafka.
        // NOT calling it means: on restart, this message will be redelivered from this offset.
        // This is the at-least-once guarantee — never lose a message, may process twice.
        Acknowledgment ack
    ) {
        log.info("Received: orderId={} topic={} partition={} offset={}",
            event.orderId(), topic, partition, offset);

        // Delegate all business logic to the service.
        // If process() throws ANY exception, execution stops here.
        // The exception propagates up to DefaultErrorHandler which decides:
        //   - NonRetryableProcessingException → DLT immediately
        //   - RetryableProcessingException / other → retry with backoff → DLT if exhausted
        processingService.process(event, topic, partition, offset);

        // ── CONSUMER ACK ────────────────────────────────────────────────────
        // Only reached on SUCCESS — after DB save AND Kafka send both completed.
        // Commits this offset to Kafka's __consumer_offsets topic.
        // Effect: if the app restarts now, it will start from offset+1 (not redeliver this one).
        // If process() threw, this line is skipped — message stays uncommitted and is redelivered.
        ack.acknowledge();

        log.debug("Acknowledged: orderId={} partition={} offset={}", event.orderId(), partition, offset);
    }
}
