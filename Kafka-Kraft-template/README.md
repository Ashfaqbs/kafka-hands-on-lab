# Kafka Production Guide

Full explanation of every production practice in this reference project.
Use this as the conceptual companion to the code.

---

## Table of Contents

1. [Why This Project Exists](#1-why-this-project-exists)
2. [Architecture Overview](#2-architecture-overview)
3. [SASL/SCRAM-SHA-512 Authentication](#3-saslscram-sha-512-authentication)
4. [Manual Acknowledgment (At-Least-Once Delivery)](#4-manual-acknowledgment-at-least-once-delivery)
5. [Retries and Exponential Backoff](#5-retries-and-exponential-backoff)
6. [Dead Letter Topic](#6-dead-letter-topic)
7. [Idempotent Consumer](#7-idempotent-consumer)
8. [Error Classification: Retryable vs Non-Retryable](#8-error-classification-retryable-vs-non-retryable)
9. [ErrorHandlingDeserializer (Poison Pill Protection)](#9-errorhandlingdeserializer-poison-pill-protection)
10. [Idempotent Producer](#10-idempotent-producer)
11. [PostgreSQL Persistence with Flyway](#11-postgresql-persistence-with-flyway)
12. [Consumer Concurrency](#12-consumer-concurrency)
13. [Docker Infrastructure (KRaft Mode)](#13-docker-infrastructure-kraft-mode)
14. [Message Flow: Step by Step](#14-message-flow-step-by-step)
15. [Production Checklist](#15-production-checklist)
16. [Porting This to Your Project](#16-porting-this-to-your-project)
17. [ZooKeeper vs KRaft — History, System Design, and Reliability](#17-zookeeper-vs-kraft--history-system-design-and-reliability)

---

## 1. Why This Project Exists

Kafka is easy to get working in development. Making it production-grade requires a specific set of decisions that compound on each other:

- Unauthenticated Kafka is fine locally but unacceptable in any real environment.
- Auto-commit offsets is convenient but silently loses messages on crashes.
- Without retries, a transient DB timeout kills the message forever.
- Without a dead letter topic, a bad message blocks the entire partition forever.
- Without idempotency, a redelivered message creates a duplicate DB record.

This project implements all of these together so you can see how they interact.

---

## 2. Architecture Overview

```
Python Producer
    │  SASL/SCRAM-SHA-512 (app-producer / producer-secret)
    │  topic: orders.raw (3 partitions)
    ▼
Kafka Broker (bitnami/kafka, KRaft mode)
    │
    ▼
Spring Boot Consumer (3 threads, one per partition)
    │  SASL/SCRAM-SHA-512 (app-consumer / consumer-secret)
    │
    ├─ 1. Idempotency check: existsByOrderId → skip if duplicate
    ├─ 2. Validate: price > 0, quantity > 0, customerId not blank
    ├─ 3. Transform: totalAmount = price × quantity
    ├─ 4. Save ProcessedOrder to PostgreSQL
    └─ 5. Send EnrichedOrderEvent to orders.enriched
              │
              └─ on failure after 3 retries → orders.raw.DLT
```

---

## 3. SASL/SCRAM-SHA-512 Authentication

### What is SASL/SCRAM?

SASL (Simple Authentication and Security Layer) is a framework for pluggable authentication. SCRAM (Salted Challenge Response Authentication Mechanism) is a specific mechanism within SASL.

SCRAM-SHA-512 works as follows:
1. Client sends a challenge request with a nonce.
2. Server responds with its own nonce, a salt, and iteration count.
3. Client computes a salted hash using SHA-512. No password crosses the wire.
4. Server verifies the hash against its stored SCRAM credential.

### Why not PLAIN?

PLAIN sends the password in cleartext (only safe with TLS). SCRAM never sends the password, making it safer even over unencrypted SASL_PLAINTEXT.

### How It Is Configured Here

**Broker side (docker-compose.yml):**
```yaml
KAFKA_CFG_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512
KAFKA_CLIENT_USERS: "app-producer,app-consumer"
KAFKA_CLIENT_PASSWORDS: "producer-secret,consumer-secret"
```

bitnami automatically creates SCRAM credentials in the KRaft metadata store on first startup. No separate setup script needed for users.

**Client side (application.yml):**
```yaml
spring.kafka.properties:
  security.protocol: SASL_PLAINTEXT
  sasl.mechanism: SCRAM-SHA-512
  sasl.jaas.config: >-
    org.apache.kafka.common.security.scram.ScramLoginModule required
    username="app-consumer"
    password="consumer-secret";
```

The producer uses separate credentials by overriding `sasl.jaas.config` under `spring.kafka.producer.properties`.

**In production:** inject credentials via environment variables:
```bash
KAFKA_USERNAME=app-consumer
KAFKA_PASSWORD=<from secrets manager>
KAFKA_PRODUCER_USERNAME=app-producer
KAFKA_PRODUCER_PASSWORD=<from secrets manager>
```

The application.yml uses `${KAFKA_USERNAME:app-consumer}` — the `:app-consumer` part is the default used in local dev.

### Two Listeners: SASL vs PLAINTEXT Internal

The broker exposes two listeners:
- `SASL_PLAINTEXT:9092` — external clients must authenticate
- `INTERNAL:9094` — PLAINTEXT, no auth, only reachable inside the Docker network

The internal listener is used for:
- Health checks (`kafka-topics.sh --bootstrap-server localhost:9094 --list`)
- The kafka-setup container creating topics

This pattern avoids a chicken-and-egg problem: you cannot authenticate via SCRAM until SCRAM users exist, but you need Kafka running to create users.

---

## 4. Manual Acknowledgment (At-Least-Once Delivery)

### The Problem with Auto-Commit

By default, Kafka consumer clients auto-commit offsets on a timer. If your app processes a message and then crashes before the timer fires, the offset is never committed. On restart, the message is redelivered — that's acceptable.

But the worse failure mode is: the consumer auto-commits the offset (marking the message as processed) and then crashes before actually processing it. The message is lost silently.

### Manual ACK Approach

```yaml
spring.kafka.consumer:
  enable-auto-commit: false
```

```java
// KafkaConsumerConfig.java
factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
```

```java
// OrderConsumer.java
public void consume(@Payload RawOrderEvent event, ..., Acknowledgment ack) {
    processingService.process(event, topic, partition, offset);
    ack.acknowledge();  // only reached on success
}
```

If `process()` throws, the `ack.acknowledge()` line is never reached. The offset stays uncommitted. The DefaultErrorHandler either retries the message or sends it to the DLT — after which it commits the offset itself.

### Delivery Guarantee

This gives **at-least-once delivery**: a message may be processed more than once (e.g., if the app crashes after saving to DB but before ACKing), but it is never silently dropped. The idempotency check (section 7) handles the duplicate case.

---

## 5. Retries and Exponential Backoff

### Why Retry?

Transient failures are common in distributed systems:
- Database briefly unreachable during a restart
- Downstream Kafka topic temporarily unavailable
- Network blip causing a timeout

Without retries, these transient failures permanently lose the message.

### Configuration

```java
// KafkaConsumerConfig.java
var backoff = new ExponentialBackOff(1_000L, 2.0);
backoff.setMaxAttempts(3);
backoff.setMaxInterval(10_000L);

var errorHandler = new DefaultErrorHandler(dlqRecoverer, backoff);
```

Retry schedule for a failing message:
- Attempt 1: immediate (the original delivery)
- Attempt 2: wait 1 second
- Attempt 3: wait 2 seconds
- Attempt 4: wait 4 seconds → send to DLT, commit offset

Total: 3 retries, then DLT. The max interval cap (10s) prevents unbounded backoff in long-running error chains.

### How DefaultErrorHandler Works

`DefaultErrorHandler` replaces the older `SeekToCurrentErrorHandler`. When a listener throws an exception:
1. It checks if the exception is in the non-retryable list.
2. If retryable (or unknown): seeks the partition back to the failed offset, waits the backoff interval, then redelivers.
3. If non-retryable or retries exhausted: calls the `DeadLetterPublishingRecoverer`.

The retry logic lives in the error handler, not in the listener. This keeps `OrderConsumer` clean — it just throws and lets the framework handle it.

---

## 6. Dead Letter Topic

### Purpose

When a message fails all retries, it goes to the Dead Letter Topic (DLT) instead of being silently dropped or blocking the partition indefinitely.

The DLT is a regular Kafka topic: `orders.raw.DLT`. You can:
- Monitor it for alerts (any message here = something went wrong)
- Replay it after fixing a bug: produce the DLT messages back to the original topic
- Inspect the headers to see the original topic, partition, offset, and exception

### Configuration

```java
var dlqRecoverer = new DeadLetterPublishingRecoverer(
    kafkaTemplate,
    (record, ex) -> new TopicPartition(record.topic() + ".DLT", 0)
);
```

The routing function receives the failed record and the exception. Here it routes everything to partition 0 of the DLT (1-partition DLT keeps ordering simple for manual inspection).

### DLT Headers

Spring Kafka automatically adds these headers to every DLT message:
- `kafka_dlt-original-topic` — which topic it came from
- `kafka_dlt-original-partition` — original partition
- `kafka_dlt-original-offset` — original offset
- `kafka_dlt-exception-fqcn` — exception class
- `kafka_dlt-exception-message` — exception message

These make root cause analysis much easier.

---

## 7. Idempotent Consumer

### The Problem

At-least-once delivery means a message can arrive more than once:
- Consumer processes message, saves to DB, then crashes before ACKing
- Kafka redelivers the same message on restart
- Without a check, you now have two identical DB rows

### Solution: Deduplication Key

```sql
-- V1__create_processed_orders.sql
CREATE TABLE processed_orders (
    id          BIGSERIAL PRIMARY KEY,
    order_id    UUID NOT NULL UNIQUE,  -- enforces idempotency at DB level
    ...
);
```

```java
// OrderProcessingService.java
if (repository.existsByOrderId(event.orderId())) {
    log.info("Duplicate message skipped: orderId={}", event.orderId());
    return;
}
```

Two layers of protection:
1. **Application check**: fast path, avoids doing any work for duplicates
2. **DB UNIQUE constraint**: safety net in case two threads race on the same orderId

The `orderId` is generated by the Python producer using `uuid4()`, so every legitimate order has a unique ID. The same order redelivered has the same UUID → caught by the idempotency check.

---

## 8. Error Classification: Retryable vs Non-Retryable

Not all errors benefit from retrying. This project distinguishes two categories:

### Retryable Errors

Transient failures that may resolve on their own:
- `DataAccessException` (DB temporarily unavailable)
- Timeout sending to `orders.enriched`
- Network-level exceptions

```java
throw new RetryableProcessingException("DB save failed: " + e.getMessage(), e);
```

These go through the full retry cycle (3 attempts with backoff), then DLT if still failing.

### Non-Retryable Errors

Permanent failures where retrying is pointless:
- Negative price (will always be invalid)
- Zero quantity (will always be invalid)
- Blank customer ID (will always be invalid)
- Logic errors

```java
throw new NonRetryableProcessingException("price must be > 0, got: " + event.unitPrice());
```

```java
// KafkaConsumerConfig.java
errorHandler.addNotRetryableExceptions(NonRetryableProcessingException.class);
```

Non-retryable exceptions skip the backoff entirely and go straight to the DLT. This prevents wasting time retrying something that will never succeed.

---

## 9. ErrorHandlingDeserializer (Poison Pill Protection)

### The Poison Pill Problem

A "poison pill" is a message that cannot be deserialized — malformed JSON, wrong schema, unexpected type. Without protection, it causes a deserialization exception on every poll, blocking the entire partition indefinitely. The retry mechanism does not help because the record never makes it to your listener.

### Solution

```yaml
spring.kafka.consumer:
  value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
  properties:
    spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
```

`ErrorHandlingDeserializer` wraps the real deserializer. If deserialization fails, it catches the exception, stores it in the `ConsumerRecord` headers, and returns a `null` payload. The listener receives `null` and the `DefaultErrorHandler` routes the record directly to the DLT — no retry loop, no partition blockage.

---

## 10. Idempotent Producer

The producer (both Python and Spring Boot's `EnrichedOrderProducer`) is configured for idempotence:

**Python:**
```python
'enable.idempotence': True,
'acks': 'all',
```

**Spring Boot (application.yml):**
```yaml
spring.kafka.producer:
  acks: all
  properties:
    enable.idempotence: true
    max.in.flight.requests.per.connection: 5
```

An idempotent producer assigns a sequence number to each message. If the broker receives a duplicate (e.g., due to a producer retry after a network timeout), it deduplicates it instead of writing twice. This gives **exactly-once semantics on the producer side**.

`acks: all` means the broker leader waits for all in-sync replicas to acknowledge before responding. This prevents message loss even if the leader crashes immediately after writing.

---

## 11. PostgreSQL Persistence with Flyway

### Why Flyway?

Flyway manages the database schema through versioned migration scripts. This means:
- Schema changes are tracked in version control alongside code
- The schema is always consistent with the app version
- `ddl-auto: validate` confirms on startup that Flyway ran correctly

### Schema Design Decisions

```sql
CREATE TABLE processed_orders (
    id              BIGSERIAL PRIMARY KEY,      -- internal PK, never exposed
    order_id        UUID NOT NULL UNIQUE,       -- from producer, idempotency key
    customer_id     VARCHAR(255) NOT NULL,
    product         VARCHAR(255) NOT NULL,
    quantity        INT NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL,
    total_amount    DECIMAL(10,2) NOT NULL,     -- computed: quantity × unit_price
    currency        VARCHAR(10) NOT NULL,
    kafka_topic     VARCHAR(255) NOT NULL,       -- audit: where did this come from?
    kafka_partition INT NOT NULL,
    kafka_offset    BIGINT NOT NULL,
    original_ts     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

The Kafka metadata columns (`kafka_topic`, `kafka_partition`, `kafka_offset`) are invaluable for debugging. When something goes wrong, you can trace any DB row back to the exact Kafka message that produced it.

---

## 12. Consumer Concurrency

```yaml
kafka.consumer.concurrency: 3
```

```java
factory.setConcurrency(3);
```

`orders.raw` has 3 partitions. With `concurrency: 3`, Spring Kafka creates 3 consumer threads, each assigned to one partition. This means:
- Messages in different partitions are processed in parallel
- Messages within a partition are processed in order (one thread per partition)

Setting concurrency higher than partitions wastes threads. Setting it lower means some partitions share a thread, reducing throughput.

---

## 13. Docker Infrastructure (KRaft Mode)

### Why KRaft Instead of ZooKeeper?

ZooKeeper was Kafka's dependency for cluster metadata management until Kafka 2.8. KRaft (Kafka Raft Metadata mode) replaces ZooKeeper with Kafka managing its own metadata via the Raft consensus algorithm.

Benefits:
- One fewer service to run and monitor
- Simpler configuration
- Better performance at scale (ZooKeeper was a bottleneck for large clusters)
- KRaft is the default in Kafka 3.x and ZooKeeper support is removed in Kafka 4.x

### bitnami/kafka vs confluentinc/cp-kafka

This project uses `bitnami/kafka:3.6` instead of the Confluent Platform image for a specific reason: cp-kafka 7.6.x has a preflight startup check (`io.confluent.admin.utils.ClusterStatus`) that detects when `java.security.auth.login.config` is set and enables SASL for its ZooKeeper health check. This makes it impossible to use SASL/SCRAM for the Kafka listeners without also enabling SASL on ZooKeeper — a circular dependency.

bitnami/kafka does not have this preflight check and natively supports KRaft mode.

### Service Roles

| Service | Purpose |
|---------|---------|
| `kafka` | Single broker, KRaft mode, SASL on port 9092, PLAINTEXT on 9094 |
| `kafka-setup` | One-shot container, creates topics, exits |
| `postgres` | PostgreSQL 17 with health check |

---

## 14. Message Flow: Step by Step

### Happy Path

1. Python producer creates a `RawOrderEvent` (UUID orderId, customerId, product, quantity, unitPrice, currency, timestamp).
2. Producer sends to `orders.raw` partition determined by Kafka default (round-robin or key hash). Auth via SCRAM-SHA-512.
3. Spring Boot consumer thread receives the message from its assigned partition.
4. `OrderConsumer.consume()` is called with the deserialized event + metadata.
5. `OrderProcessingService.process()`:
   a. `existsByOrderId()` → false (not a duplicate), continue
   b. Validate: price > 0, quantity > 0, customerId not blank → passes
   c. Compute `totalAmount = 2 × 1999.99 = 3999.98`
   d. Build `ProcessedOrder` entity with Kafka metadata
   e. `repository.save()` — INSERT with UNIQUE constraint on order_id
   f. Build `EnrichedOrderEvent` with totalAmount + status + processedAt
   g. `enrichedOrderProducer.send()` — synchronous send to `orders.enriched`
6. Back in `OrderConsumer`: `ack.acknowledge()` — offset committed.

### Failure Path: Transient DB Error

1. Same as above through step 5e, but `repository.save()` throws `DataAccessException`.
2. `OrderProcessingService` wraps it as `RetryableProcessingException`, throws.
3. `ack.acknowledge()` is never called.
4. `DefaultErrorHandler` catches the exception, seeks the partition back to the failed offset.
5. After 1 second backoff, the message is redelivered to the listener.
6. If it fails again: 2 second backoff, retry.
7. After 3 retries: `DeadLetterPublishingRecoverer` publishes to `orders.raw.DLT` with error headers.
8. `DefaultErrorHandler` commits the offset on the original topic. Processing moves on.

### Failure Path: Poison Pill (Bad JSON)

1. Producer sends malformed JSON to `orders.raw`.
2. `ErrorHandlingDeserializer` catches deserialization exception. Returns null payload with error in headers.
3. `DefaultErrorHandler` detects the null payload + deserialization header, routes to `orders.raw.DLT` immediately (no retry).
4. Offset is committed. Partition continues processing subsequent messages.

---

## 15. Production Checklist

### Security
- [ ] SASL/SCRAM-SHA-512 on all client listeners
- [ ] Credentials injected via env vars (`KAFKA_USERNAME`, `KAFKA_PASSWORD`)
- [ ] TLS (SSL) wrapping SASL for data in transit encryption (SASL_SSL instead of SASL_PLAINTEXT)
- [ ] Kafka ACLs restricting which users can read/write which topics
- [ ] No credentials in source code or committed config files

### Reliability
- [ ] Manual ACK (`enable-auto-commit: false`, `AckMode.MANUAL_IMMEDIATE`)
- [ ] DefaultErrorHandler with ExponentialBackOff
- [ ] Dead letter topic with at least 1 partition
- [ ] Idempotent consumer (UNIQUE constraint on deduplication key)
- [ ] Idempotent producer (`enable.idempotence=true`, `acks=all`)
- [ ] ErrorHandlingDeserializer to handle poison pills

### Observability
- [ ] Log retry attempts with orderId, attempt number, exception message
- [ ] Monitor consumer lag (messages in partition minus current offset)
- [ ] Alert on DLT topic having any messages
- [ ] Log Kafka metadata (topic, partition, offset) in every processed record
- [ ] Structured logging with correlation IDs

### Performance
- [ ] Concurrency = partition count
- [ ] `max.poll.records` tuned to your throughput/latency requirements
- [ ] Batch size and linger.ms tuned for the producer
- [ ] Connection pool sized for peak load (HikariCP `maximum-pool-size`)

### Operations
- [ ] Graceful shutdown (`shutdownTimeout` configured)
- [ ] Health check endpoints exposed (Actuator `/health`)
- [ ] DLT replay procedure documented
- [ ] Flyway migrations tested before deploy

---

## 16. Porting This to Your Project

### Step 1: Define your topics

```yaml
KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "false"
```

Create topics explicitly in a setup step (see `kafka-setup` in docker-compose.yml). Auto-creation leads to misconfigured topics in production.

Topic naming: `<domain>.<entity>.<event>` — e.g., `payments.invoice.created`
DLT naming: append `.DLT` — e.g., `payments.invoice.created.DLT`

### Step 2: Copy the Kafka config classes

- `KafkaConsumerConfig.java` — configure `DefaultErrorHandler`, `DeadLetterPublishingRecoverer`, `ExponentialBackOff`, manual ACK
- `KafkaProducerConfig.java` — configure `acks=all`, `enable.idempotence=true`
- `KafkaTopicConfig.java` — `NewTopic` beans (Spring creates them on startup if missing)

Change the SASL credentials and group ID. Everything else carries over.

### Step 3: Define your message DTOs

```java
@JsonIgnoreProperties(ignoreUnknown = true)
public record YourEvent(UUID eventId, String customerId, ...) {}
```

The `@JsonIgnoreProperties(ignoreUnknown = true)` allows the schema to evolve (new fields added by producer) without breaking the consumer.

### Step 4: Create your listener

```java
@KafkaListener(topics = "${kafka.topics.your-topic}", containerFactory = "kafkaListenerContainerFactory")
public void consume(@Payload YourEvent event, ..., Acknowledgment ack) {
    yourService.process(event, topic, partition, offset);
    ack.acknowledge();
}
```

No business logic here. Delegate to a service. Let exceptions propagate.

### Step 5: Implement your service

```java
@Transactional
public void process(YourEvent event, ...) {
    if (repository.existsByEventId(event.eventId())) return;   // idempotency
    validate(event);                                            // throw NonRetryable if invalid
    YourEntity entity = transform(event);                       // business logic
    repository.save(entity);                                    // throw Retryable on DataAccessException
    downstreamProducer.send(toDownstreamEvent(entity));         // throw Retryable on timeout
}
```

### Step 6: Add your exception hierarchy

```java
public class RetryableProcessingException extends RuntimeException { ... }
public class NonRetryableProcessingException extends RuntimeException { ... }
```

Register `NonRetryableProcessingException` in `DefaultErrorHandler`:
```java
errorHandler.addNotRetryableExceptions(NonRetryableProcessingException.class);
```

### Step 7: Add a UNIQUE constraint for idempotency

```sql
ALTER TABLE your_table ADD CONSTRAINT uq_event_id UNIQUE (event_id);
```

This is your safety net if two threads race on the same eventId.

### Step 8: Write unit tests first

Test the service in isolation with mocked repository and producer (see `OrderProcessingServiceTest`):
- Happy path
- Duplicate message (should skip)
- Each validation failure (should throw NonRetryable)
- DB failure (should throw Retryable)

### Step 9: Add a Testcontainers integration test

For full end-to-end confidence, use `@EmbeddedKafka` or Testcontainers with a real Kafka and PostgreSQL to verify:
- Message is consumed and saved to DB
- Duplicate is skipped
- Bad message goes to DLT
- Retries happen on transient failure

---

## 17. ZooKeeper vs KRaft — History, System Design, and Reliability

---

### The Problem Kafka Had to Solve

When Kafka was created at LinkedIn around 2010, it needed a way to answer these questions at all times:

- Which brokers are currently alive?
- Which broker is the leader for each partition?
- What topics and partitions exist?
- Which consumer groups exist and what offsets have they committed?

This is the **cluster metadata problem** — every distributed system needs a reliable, consistent store for metadata that all nodes can trust. Kafka's original answer was: outsource it to ZooKeeper.

---

### ZooKeeper — How It Worked

ZooKeeper is a separate distributed coordination service originally built at Yahoo. It provides a hierarchical key-value store (like a filesystem of znodes) with strong consistency guarantees. Kafka used it as its metadata backbone from 2011 to 2022.

**What ZooKeeper stored for Kafka:**

```
/brokers/ids/1          → broker 1 is alive, host/port
/brokers/ids/2          → broker 2 is alive
/brokers/topics/orders  → partition count, replica assignment
/controller             → which broker is currently the active controller
/consumers/group-1/...  → consumer group offsets (old clients only)
/config/topics/orders   → topic configuration overrides
```

**How Kafka used ZooKeeper at runtime:**

1. On startup, each broker registers itself under `/brokers/ids/<id>` as an ephemeral znode. Ephemeral means: when the broker's ZooKeeper session dies (broker crash), ZooKeeper automatically deletes the znode. Other brokers watch this path and immediately know a broker went down.

2. One broker is elected the **Kafka Controller** by racing to create `/controller` as an ephemeral znode. The first broker to create it wins and becomes the controller. The controller watches all broker registrations and handles partition leadership assignments.

3. When a broker dies, ZooKeeper notifies the controller. The controller then reassigns partition leaders for all partitions that were led by the dead broker and writes the new assignments back to ZooKeeper. All other brokers watch ZooKeeper for these changes and update their routing tables.

4. Clients (producers and consumers) also talked directly to ZooKeeper to discover which broker led which partition, then connected to that broker directly.

**ZooKeeper's consistency model — ZAB protocol:**

ZooKeeper uses ZAB (ZooKeeper Atomic Broadcast), which is similar to Paxos. It elects a ZooKeeper leader among the ZooKeeper ensemble (typically 3 or 5 nodes). All writes go through the ZooKeeper leader, which replicates them to a majority of followers before acknowledging. Reads can be served by any node. This gives linearizable writes and eventual consistency on reads.

---

### What Was Wrong With ZooKeeper

ZooKeeper worked but it introduced serious operational and architectural problems as Kafka scaled:

**1. Two systems to operate.**
Every Kafka deployment needed a ZooKeeper ensemble alongside it. That means separate servers (or VMs), separate monitoring, separate security config, separate backup strategy, separate upgrades. If ZooKeeper had a problem, Kafka had a problem — and vice versa.

**2. Scalability ceiling on partitions.**
Every partition leader change, broker registration, and topic config update was a ZooKeeper write. ZooKeeper's throughput is limited (a few thousand writes per second at most). In practice, Kafka clusters were limited to tens of thousands of partitions before ZooKeeper became the bottleneck. LinkedIn and other large users hit this ceiling.

**3. Controller bottleneck and slow failover.**
The Kafka controller read all metadata from ZooKeeper at startup and cached it in memory. When the controller itself crashed, the new controller had to read the entire metadata state from ZooKeeper — which could take 30–60 seconds for large clusters. During this time no partition leader elections could happen. For a cluster with millions of partitions this was catastrophic.

**4. Split-brain risk.**
If the Kafka controller lost its ZooKeeper session (network partition), another broker could become the controller. For a brief period, two brokers thought they were the controller — a split-brain. ZooKeeper's fencing mechanisms reduced this risk but didn't eliminate it entirely.

**5. Security complexity.**
Securing Kafka required securing both the Kafka brokers and the ZooKeeper ensemble separately. SASL, TLS, and ACLs had to be configured on both systems independently.

**6. Metadata lag.**
Brokers discovered partition changes by watching ZooKeeper. In large clusters this watch notification fan-out could cause thousands of watchers to fire simultaneously, spiking ZooKeeper load and causing notification delays.

---

### KRaft — Kafka's Answer (KIP-500)

In 2019, the Kafka community proposed **KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum**. The goal: make Kafka manage its own metadata using Kafka itself.

The name **KRaft** = **K**afka + **Raft**. Raft is the consensus algorithm KRaft uses instead of ZAB.

KRaft reached production readiness in **Kafka 3.3** (2022) and ZooKeeper support was fully removed in **Kafka 4.0** (2024). This project uses `apache/kafka:3.7.0` — fully KRaft, no ZooKeeper.

---

### Raft — The Consensus Algorithm Behind KRaft

Raft was designed in 2013 (Diego Ongaro, Stanford) explicitly to be easier to understand than Paxos while providing the same guarantees. It is now one of the most widely used consensus algorithms: used in etcd (Kubernetes), CockroachDB, TiKV, and now Kafka.

**The core problem Raft solves:**

How do N nodes agree on a sequence of values (a log) even when some nodes crash or messages are delayed, without any two nodes ever disagreeing?

**Raft's answer — three roles:**

```
LEADER      — receives all writes, replicates to followers, commits when majority acks
FOLLOWER    — receives replicated entries from leader, votes in elections
CANDIDATE   — a follower that started an election and is campaigning for leader
```

At any point in time there is exactly one leader (or no leader during an election). All reads and writes go through the leader. This is simpler than Paxos where any node can propose values.

---

### How KRaft Leader Election Works

**Step 1 — Terms.**
Time is divided into terms, numbered monotonically. Term 1, term 2, term 3... A new term begins whenever an election starts. Terms act as logical clocks — a node rejects any message from a lower term than its own.

**Step 2 — Heartbeats and election timeout.**
The leader sends heartbeats to all followers every few milliseconds. Each follower has an **election timeout** (randomized between 150–300ms). If a follower doesn't receive a heartbeat before its timeout fires, it assumes the leader is dead and starts an election.

**Why randomized?** If all timeouts were the same, all followers would start an election simultaneously, splitting votes and forcing multiple rounds. Randomization means one follower usually times out first and wins cleanly.

**Step 3 — Candidate campaigns.**
The timed-out follower increments its term to T+1, transitions to CANDIDATE, votes for itself, and sends `RequestVote` RPCs to all other nodes:

```
"I want to be leader for term 5.
 My log is up to index 120, term 4.
 Will you vote for me?"
```

**Step 4 — Voting rules.**
A node grants a vote if:
- It hasn't voted for anyone else in this term yet (one vote per term)
- The candidate's log is at least as up-to-date as the voter's log (prevents electing a stale node)

Log freshness is compared by: higher last term wins; if same term, longer log wins.

**Step 5 — Majority wins.**
The candidate that receives votes from a majority (⌊N/2⌋ + 1) of nodes becomes the new leader. With 3 nodes: needs 2 votes. With 5 nodes: needs 3 votes.

**Step 6 — Leader takes over.**
The new leader immediately starts sending heartbeats to suppress other election timeouts. It then replicates any uncommitted log entries from the previous term to bring all followers up to date.

**What happens if two candidates tie?** Both fail to reach majority. Their election timeouts fire (randomized again), one starts a new election first, and wins. Raft guarantees this eventually terminates.

---

### The KRaft Metadata Log

In KRaft, metadata (broker registrations, topic configs, partition assignments, SCRAM credentials) is stored as **events in a Kafka topic** called `__cluster_metadata`. This is a regular Kafka topic with one partition, replicated across all controller-eligible nodes.

```
__cluster_metadata partition 0:
  offset 0:  RegisterBrokerRecord   {brokerId: 1, host: kafka1, port: 9092}
  offset 1:  RegisterBrokerRecord   {brokerId: 2, host: kafka2, port: 9092}
  offset 2:  RegisterBrokerRecord   {brokerId: 3, host: kafka3, port: 9092}
  offset 3:  TopicRecord            {name: orders.raw, topicId: xyz, partitions: 3}
  offset 4:  PartitionRecord        {topicId: xyz, partition: 0, leader: 1, replicas: [1,2,3]}
  offset 5:  PartitionRecord        {topicId: xyz, partition: 1, leader: 2, replicas: [2,3,1]}
  offset 6:  PartitionRecord        {topicId: xyz, partition: 2, leader: 3, replicas: [3,1,2]}
  offset 7:  UserScramCredentialRecord {name: app-producer, mechanism: SCRAM-SHA-512}
  ...
```

This is the key architectural insight: **metadata is just another Kafka log**. Every broker subscribes to `__cluster_metadata` and maintains a local in-memory cache of the current state. When the controller writes a metadata change, it's just appending a record to this topic. Followers replicate it through the normal Kafka replication mechanism.

---

### Leader / Follower Model — Partition Level

KRaft has two distinct leader/follower hierarchies that are easy to confuse:

**1. Controller leader/follower (cluster-wide metadata):**

```
Controller quorum (3 nodes in this project):
  kafka1 ── LEADER controller  (term 3)
  kafka2 ── FOLLOWER
  kafka3 ── FOLLOWER
```

The controller leader is the only node that writes to `__cluster_metadata`. It handles all admin operations: create topic, elect partition leader, register broker. Followers replicate the metadata log passively and stand by to take over via election if the controller leader dies.

**2. Partition leader/follower (per-topic data):**

```
orders.raw partition 0:
  kafka2 ── LEADER   (producers write here, consumers read from here)
  kafka3 ── FOLLOWER (replicates from leader)
  kafka1 ── FOLLOWER (replicates from leader)

orders.raw partition 1:
  kafka3 ── LEADER
  kafka1 ── FOLLOWER
  kafka2 ── FOLLOWER

orders.raw partition 2:
  kafka1 ── LEADER
  kafka2 ── FOLLOWER
  kafka3 ── FOLLOWER
```

Notice: the partition leaders are spread across all 3 brokers. This is deliberate — Kafka distributes leadership so no single broker handles all the write traffic. This is what you saw in the `kafka-topics.sh --describe` output earlier:

```
Partition 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
Partition 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
Partition 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```

These two layers are independent. The controller leader assigns partition leaders — it does not have to be a partition leader itself.

---

### ISR — In-Sync Replicas

ISR (In-Sync Replica set) is one of the most important concepts for understanding Kafka reliability.

Every partition leader tracks which followers are "in sync" — meaning they have replicated up to within a configurable lag (`replica.lag.time.max.ms`, default 30 seconds) of the leader.

```
orders.raw partition 0:
  Leader: kafka2   (has offset 1000)
  kafka3: offset 998  → within lag → IN ISR
  kafka1: offset 999  → within lag → IN ISR
  ISR = [kafka2, kafka3, kafka1]
```

If a follower falls behind:

```
  kafka1: offset 200  → way behind → REMOVED from ISR
  ISR = [kafka2, kafka3]
```

**Why ISR matters for producers:**

`acks=all` (used in this project) means: the leader only ACKs the producer after ALL ISR members have written the record. So:
- ISR has 3 brokers → all 3 must write before producer gets ACK
- ISR shrinks to 2 (one broker slow) → only 2 must write → producer ACK is faster but durability reduced
- ISR shrinks to 1 (only leader) → leader alone ACKs → producer is fast but 0 redundancy

`min.insync.replicas` (set to 2 in this project's docker-compose) is the safety floor: if ISR drops below 2, producers with `acks=all` get a `NotEnoughReplicasException` instead of a false ACK. This prevents data loss at the cost of availability.

**The durability guarantee with this project's config:**
- 3 replicas, `acks=all`, `min.insync.replicas=2`
- A message is acknowledged only when at least 2 brokers have it
- You can lose 1 broker at any time without losing any acknowledged message
- If 2 brokers die simultaneously, the cluster stops accepting writes (NotEnoughReplicas) rather than risk losing data

---

### Partition Leader Election in KRaft

When a partition leader dies, the **controller** (not ZooKeeper) handles the election:

1. Controller detects broker is dead (missed heartbeats in the `__cluster_metadata` replication stream)
2. Controller appends a `PartitionChangeRecord` to `__cluster_metadata`, promoting the first ISR member as new leader
3. All brokers receive this update (they all follow `__cluster_metadata`)
4. Producers and consumers discover the new leader on their next metadata refresh

The key difference from ZooKeeper: in ZooKeeper mode, the controller had to read from ZooKeeper, compute, and write back. In KRaft mode, the controller just appends to a log it already owns — microseconds instead of milliseconds, and no external system involved.

---

### Unclean Leader Election

What if all ISR members are dead and only out-of-sync replicas remain?

- **`unclean.leader.election.enable=false`** (default, used here): refuse to elect an out-of-sync replica. Partition stays offline until an ISR member recovers. No data loss, but availability is sacrificed.
- **`unclean.leader.election.enable=true`**: elect the out-of-sync replica. Partition comes back online but messages written to the old leader that weren't replicated are permanently lost.

For financial data, orders, payments: always keep this false. For metrics or logs where some loss is acceptable: can consider true.

---

### KRaft vs ZooKeeper — Performance Comparison

| Aspect | ZooKeeper mode | KRaft mode |
|--------|---------------|------------|
| Controller failover time | 30–60s (reload all metadata from ZK) | <1s (metadata already in local log) |
| Max practical partitions | ~200,000 | Millions (tested at 10M+) |
| Metadata write latency | ZK round-trip + Kafka write | Single log append |
| Operational complexity | 2 systems (Kafka + ZK ensemble) | 1 system |
| Security surface | Kafka ACLs + ZK ACLs separately | Kafka ACLs only |
| Startup time (large cluster) | Minutes (ZK read) | Seconds (local log replay) |

The partition count improvement is the most significant for large organizations. LinkedIn was reportedly running hundreds of thousands of partitions and hitting ZooKeeper scalability limits — KRaft removes that ceiling entirely.

---

### How This Project Uses KRaft

In `docker-compose.yml`:

```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka1:9093,2@kafka2:9193,3@kafka3:9293"
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
```

All 3 brokers are both **brokers** (serve producers/consumers) and **controllers** (participate in Raft quorum for metadata). This is called **combined mode** — appropriate for small clusters. In large production clusters you'd run dedicated controller nodes (3–5 nodes that are controllers only, not serving client traffic) and many broker-only nodes.

The `CLUSTER_ID` (fixed value `MkU3OEVBNTcwNTJENDM2Qg`) is a base64url-encoded UUID that identifies this cluster's Raft log. All brokers must share the same CLUSTER_ID — this is what `kafka-storage.sh format` uses to initialize the metadata log on first startup.

**SCRAM credentials in KRaft:**
When `kafka-setup` runs:
```bash
kafka-configs.sh --bootstrap-server kafka1:9094 \
  --alter --add-config 'SCRAM-SHA-512=[...]' \
  --entity-type users --entity-name app-producer
```

This writes a `UserScramCredentialRecord` to `__cluster_metadata`. All 3 brokers receive it via Raft replication. When a client authenticates on any broker using SCRAM, that broker looks up the credential in its local metadata cache — no cross-broker call needed.

---

### Reliability Summary

```
Can lose 1 of 3 brokers:
  ✓ Raft quorum maintained (2 of 3 = majority)
  ✓ Partition leaders re-elected by surviving controller
  ✓ ISR still has 2 replicas (meets min.insync.replicas=2)
  ✓ Producers can still write (acks=all satisfied by 2 replicas)
  ✓ Consumers can still read from surviving partition leaders

Can lose 2 of 3 brokers:
  ✗ Raft quorum lost (1 of 3 < majority) → no controller elections
  ✗ ISR below min.insync.replicas → producers get NotEnoughReplicasException
  ✓ Consumers can still read already-committed data from the surviving broker
  ✓ No data loss for already-acknowledged messages
  ✗ No new data can be written until a second broker recovers
```

This is the standard trade-off of any majority-quorum system: tolerate ⌊N/2⌋ failures. With 3 nodes you tolerate 1 failure. With 5 nodes you tolerate 2 failures. Running 5 controller nodes in production is common for this reason.
