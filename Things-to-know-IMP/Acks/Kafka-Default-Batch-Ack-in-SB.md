## Kafka Default Batch Acknowledgment in Spring Boot

- By default, Kafka in Spring Boot uses `enable.auto.commit=true`, meaning the consumer automatically commits the offsets after a batch of messages is processed.
- This is done at a configured interval (`auto.commit.interval.ms`), not immediately after message processing.

**Example Scenario:**
- Topic with 2-3 brokers and multiple partitions.
- Kafka consumer fetches 100 messages in a batch.
- The consumer processes 50 messages and crashes.
- Since the offset auto-commits at the interval, Kafka considers all 100 messages as consumed.
- Result: 50 unprocessed messages are lost.

**Manual Acknowledgment:**
- To avoid such message loss, Spring Boot allows manual acknowledgment.
- By setting `enable.auto.commit=false` and explicitly calling `acknowledge.acknowledge()` after processing each message, the consumer commits offsets safely.
