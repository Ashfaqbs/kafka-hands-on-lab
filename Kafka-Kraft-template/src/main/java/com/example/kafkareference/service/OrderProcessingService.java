package com.example.kafkareference.service;

import com.example.kafkareference.domain.ProcessedOrder;
import com.example.kafkareference.dto.EnrichedOrderEvent;
import com.example.kafkareference.dto.RawOrderEvent;
import com.example.kafkareference.exception.NonRetryableProcessingException;
import com.example.kafkareference.exception.RetryableProcessingException;
import com.example.kafkareference.producer.EnrichedOrderProducer;
import com.example.kafkareference.repository.ProcessedOrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

/**
 * Core processing logic for each order message consumed from orders.raw.
 *
 * @Service — stereotype annotation. Marks this class as a Spring-managed service bean.
 *   Functionally the same as @Component, but communicates intent:
 *   this is business logic, not a web controller or data access layer.
 *   Spring scans for @Service and creates a singleton instance at startup.
 *
 * ─── What is persisted where ────────────────────────────────────────────────
 *
 *   PostgreSQL (processed_orders table):
 *     The ProcessedOrder entity — contains the full order fields, computed
 *     totalAmount, and Kafka metadata (topic/partition/offset for traceability).
 *     Written in step 4 (repository.save).
 *
 *   Kafka (orders.enriched topic):
 *     The EnrichedOrderEvent — a lightweight event with the same data plus
 *     computed totalAmount, status, and processedAt.
 *     Written in step 5 (producer.send). Broker ACK confirms durable storage.
 *
 *   Kafka (__consumer_offsets topic):
 *     The offset commit — happens in OrderConsumer after this method returns.
 *     NOT written here. Marks "this message is processed, don't redeliver it."
 *
 * ─── Why save to DB before sending to Kafka ─────────────────────────────────
 *
 *   DB is the source of truth. If we wrote to Kafka first and DB failed, the
 *   downstream system would have data we don't — a split-brain inconsistency.
 *   Saving to DB first means: if the Kafka send fails, the order is safely in
 *   the DB and the error handler retries the entire Kafka message. On retry,
 *   the idempotency check (step 1) skips the duplicate DB save and retries
 *   only the Kafka send — avoiding double-writes.
 */
// @Service — Spring creates one shared instance (singleton) injected wherever needed.
@Service
public class OrderProcessingService {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    private final ProcessedOrderRepository repository;
    private final EnrichedOrderProducer producer;

    public OrderProcessingService(ProcessedOrderRepository repository, EnrichedOrderProducer producer) {
        this.repository = repository;
        this.producer = producer;
    }

    // @Transactional — wraps this method in a database transaction.
    //
    //   BEGIN TRANSACTION is issued before the method starts.
    //   COMMIT is issued when the method returns normally.
    //   ROLLBACK is issued if a RuntimeException propagates out.
    //
    //   What this means here:
    //   - If repository.save() succeeds but producer.send() throws → transaction ROLLS BACK
    //     → DB save is undone → error handler retries the whole message from step 1.
    //   - If producer.send() succeeds but DB save threw first → already rolled back,
    //     nothing to undo.
    //
    //   Important: @Transactional is on the SERVICE method, not the repository.
    //   Keeping transactions in the service layer means you control exactly what
    //   is in scope — controller or consumer calls are outside the transaction.
    //
    //   Scope: covers steps 1–4 only (repository.save).
    //   The producer.send() in step 5 is OUTSIDE the DB transaction because Kafka
    //   does not participate in JPA transactions (no XA/distributed transaction here).
    @Transactional
    public void process(RawOrderEvent event, String kafkaTopic, int kafkaPartition, long kafkaOffset) {

        // ── Step 1: Idempotency check ────────────────────────────────────────
        //
        // Kafka at-least-once delivery: the same message can arrive more than once.
        // Common causes:
        //   - Consumer crashes after DB save but before ack.acknowledge()
        //   - Consumer group rebalance during processing
        //   - Broker failover causing offset reset
        //
        // First line of defense: skip if we've already processed this orderId.
        // Second line of defense: the UNIQUE constraint on order_id in PostgreSQL —
        //   even if two threads pass this check simultaneously, only one INSERT wins.
        //
        // This check is an optimistic early exit to avoid an unnecessary INSERT attempt.
        if (repository.existsByOrderId(event.orderId())) {
            log.warn("Skipping duplicate message: orderId={} topic={} partition={} offset={}",
                event.orderId(), kafkaTopic, kafkaPartition, kafkaOffset);
            return;
            // Returns normally → OrderConsumer calls ack.acknowledge() → offset committed.
            // The duplicate is discarded cleanly without going to DLT.
        }

        // ── Step 2: Validate ─────────────────────────────────────────────────
        //
        // NonRetryableProcessingException is thrown for bad data that will NEVER
        // succeed regardless of how many times we retry.
        //
        // The DefaultErrorHandler checks for this exception type and skips
        // the retry loop entirely — the message goes straight to the DLT.
        // This prevents wasting 3 retry slots (and 7 seconds of backoff) on
        // something we know will fail every time.
        validate(event);

        // ── Step 3: Transform ────────────────────────────────────────────────
        //
        // Compute totalAmount using BigDecimal to avoid floating-point rounding errors.
        // Never use double for monetary calculations — 0.1 + 0.2 = 0.30000000000000004 in double.
        BigDecimal price = BigDecimal.valueOf(event.price()).setScale(2, RoundingMode.HALF_UP);
        BigDecimal totalAmount = price.multiply(BigDecimal.valueOf(event.quantity()));

        EnrichedOrderEvent enriched = new EnrichedOrderEvent(
            event.orderId(),
            event.customerId(),
            event.product(),
            event.quantity(),
            event.price(),
            totalAmount.doubleValue(),
            event.currency() != null ? event.currency() : "USD",
            "PROCESSED",
            Instant.now()
        );

        // ── Step 4: Save to PostgreSQL ───────────────────────────────────────
        //
        // This INSERT is inside the @Transactional scope.
        // On success: the row exists in DB (but transaction not committed yet — committed
        //   when this method returns successfully after step 5).
        // On DataAccessException: wrapped as RetryableProcessingException so the
        //   error handler retries with backoff. DB errors are typically transient:
        //   connection pool exhausted, brief network partition, deadlock, timeout.
        //
        // After retry exhaustion or NonRetryable: DefaultErrorHandler sends to DLT,
        // then commits the Kafka offset so the partition moves forward.
        ProcessedOrder entity = toEntity(enriched, kafkaTopic, kafkaPartition, kafkaOffset);
        try {
            repository.save(entity);
            // ← At this point the row is written to the DB connection's in-flight transaction.
            //   Not yet committed to disk — commit happens when this method returns.
            log.info("Saved: orderId={} totalAmount={} topic={} partition={} offset={}",
                enriched.orderId(), enriched.totalAmount(), kafkaTopic, kafkaPartition, kafkaOffset);
        } catch (DataAccessException e) {
            throw new RetryableProcessingException("DB save failed for orderId=" + event.orderId(), e);
        }

        // ── Step 5: Send to orders.enriched ─────────────────────────────────
        //
        // Runs AFTER the DB save succeeds but BEFORE the @Transactional commit.
        //
        // producer.send() blocks until the broker confirms receipt (acks=all = all
        // ISR replicas have written the message). This is the PRODUCER/BROKER ACK.
        //
        // ── PRODUCER ACK: what it means ─────────────────────────────────────
        //   .get(10s) returns a SendResult containing:
        //     - topic   = "orders.enriched"
        //     - partition = which partition it landed on (orderId hash → partition)
        //     - offset   = position in that partition's log
        //   This confirms: the message is durably written to Kafka's orders.enriched topic.
        //   Downstream consumers of orders.enriched will see this message.
        //   Stored in: Kafka's orders.enriched log. NOT written to PostgreSQL again.
        //
        // If send() throws RetryableProcessingException:
        //   → @Transactional rolls back the DB save (because RuntimeException propagates)
        //   → error handler retries the whole Kafka message from step 1
        //   → step 1 skips the duplicate (order not in DB because it was rolled back)
        //   → steps 4 and 5 are retried cleanly
        producer.send(enriched);

        // ← Method returns normally here.
        // @Transactional COMMITS the DB transaction now.
        // Back in OrderConsumer: ack.acknowledge() commits the Kafka offset.
        // Both the DB row and the Kafka consumer offset are now persisted.
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    private void validate(RawOrderEvent event) {
        if (event.orderId() == null) {
            throw new NonRetryableProcessingException("orderId is null");
        }
        if (event.customerId() == null || event.customerId().isBlank()) {
            throw new NonRetryableProcessingException("customerId is blank for orderId=" + event.orderId());
        }
        if (event.product() == null || event.product().isBlank()) {
            throw new NonRetryableProcessingException("product is blank for orderId=" + event.orderId());
        }
        if (event.quantity() <= 0) {
            throw new NonRetryableProcessingException("quantity must be > 0, got " + event.quantity()
                + " for orderId=" + event.orderId());
        }
        if (event.price() <= 0) {
            throw new NonRetryableProcessingException("price must be > 0, got " + event.price()
                + " for orderId=" + event.orderId());
        }
    }

    private ProcessedOrder toEntity(EnrichedOrderEvent e, String topic, int partition, long offset) {
        ProcessedOrder order = new ProcessedOrder();
        order.setOrderId(e.orderId());
        order.setCustomerId(e.customerId());
        order.setProduct(e.product());
        order.setQuantity(e.quantity());
        order.setPrice(BigDecimal.valueOf(e.price()).setScale(2, RoundingMode.HALF_UP));
        order.setTotalAmount(BigDecimal.valueOf(e.totalAmount()).setScale(2, RoundingMode.HALF_UP));
        order.setCurrency(e.currency());
        order.setStatus(e.status());
        order.setKafkaTopic(topic);
        order.setKafkaPartition(partition);
        order.setKafkaOffset(offset);
        order.setProcessedAt(e.processedAt());
        return order;
    }
}
