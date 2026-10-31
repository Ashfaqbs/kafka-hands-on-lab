# kafka-reference

Reference project: production-grade Kafka consumer with SASL/SCRAM-SHA-512 auth, manual ACK, retries, dead-letter topic, idempotency, and PostgreSQL persistence.

> Full conceptual guide, production practices explained, and porting instructions: **[KAFKA-GUIDE.md](./KAFKA-GUIDE.md)**

## Tech Stack
- Java 17 + Spring Boot 3.3.4
- Spring Kafka 3.x
- PostgreSQL 17 (Docker)
- Apache Kafka 3.7 via `apache/kafka:3.7.0` (KRaft mode, no ZooKeeper)
- Flyway migrations
- Python 3.x + confluent-kafka (producer)

## Message Flow
```
Python Producer
    │  SASL/SCRAM-SHA-512
    ▼
orders.raw  (3 partitions)
    │
    ▼
Spring Consumer (3 threads, one per partition)
    │
    ├─ idempotency check (existsByOrderId)
    ├─ validate
    ├─ transform (compute totalAmount)
    ├─ save to PostgreSQL
    └─ send to orders.enriched
         │
         └─ on failure after 3 retries → orders.raw.DLT
```

## How to Run

### 1. Start infrastructure
```bash
docker-compose up -d
# Wait for kafka-setup to finish (it exits when done)
docker-compose logs kafka-setup
```

### 2. Run Spring Boot app
```bash
mvn spring-boot:run
# App connects to Kafka + PostgreSQL and starts consuming
```

### 3. Run Python producer
```bash
cd producer
pip install -r requirements.txt
python producer.py --count 20 --interval 0.3
```

### Watch it work
```bash
# See messages arriving in Spring Boot logs
# Check DB:
docker exec -it postgres psql -U kafkaapp -d kafka_reference \
  -c "SELECT order_id, product, total_amount, kafka_partition, kafka_offset FROM processed_orders ORDER BY created_at DESC LIMIT 10;"
```

## Project Structure
```
kafka-reference/
├── docker-compose.yml              — Kafka + ZooKeeper + PostgreSQL
├── kafka-config/
│   └── kafka_server_jaas.conf      — Broker JAAS: enables SCRAM on EXTERNAL listener
├── producer/
│   ├── producer.py                 — Python SASL producer
│   └── requirements.txt
└── src/main/java/com/example/kafkareference/
    ├── config/
    │   ├── KafkaConsumerConfig.java — MANUAL ACK, DefaultErrorHandler, retries, DLT
    │   ├── KafkaProducerConfig.java — acks=all, idempotence, SASL
    │   └── KafkaTopicConfig.java   — topic beans (auto-created on startup)
    ├── consumer/
    │   └── OrderConsumer.java      — @KafkaListener, delegates to service, ACKs on success
    ├── producer/
    │   └── EnrichedOrderProducer.java — synchronous send to orders.enriched
    ├── service/
    │   └── OrderProcessingService.java — idempotency, validate, transform, save, send
    ├── domain/
    │   └── ProcessedOrder.java
    ├── repository/
    │   └── ProcessedOrderRepository.java
    ├── dto/
    │   ├── RawOrderEvent.java      — incoming message (from Python producer)
    │   └── EnrichedOrderEvent.java — outgoing message (to orders.enriched)
    └── exception/
        ├── RetryableProcessingException.java    — triggers retry + eventually DLT
        └── NonRetryableProcessingException.java — goes straight to DLT, no retry
```

## SASL Credentials
| User | Password | Used by |
|------|----------|---------|
| app-producer | producer-secret | Python producer |
| app-consumer | consumer-secret | Spring Boot consumer |

In production: inject via env vars `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_PRODUCER_USERNAME`, `KAFKA_PRODUCER_PASSWORD`.

## Topics
| Topic | Partitions | Purpose |
|-------|-----------|---------|
| orders.raw | 3 | Raw orders from Python producer |
| orders.enriched | 3 | Enriched orders after processing |
| orders.raw.DLT | 1 | Failed messages after 3 retries |

## Key Config Values (application.yml)
```yaml
kafka.consumer.concurrency: 3          # threads = partitions
kafka.consumer.retry.max-attempts: 3   # retries before DLT
kafka.consumer.retry.initial-interval-ms: 1000  # 1s → 2s → 4s
```
