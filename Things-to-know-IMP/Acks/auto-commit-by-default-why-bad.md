In Spring Boot with Kafka, when we don’t configure manual acknowledgment, Kafka uses **auto-commit** by default.  

1. **Default Kafka Acknowledgment (Auto-Commit)**:  
   - Spring Kafka’s default is to commit offsets automatically after messages are polled.  
   - This happens in **batch-wise mode**, meaning once the batch of records is processed, offsets are committed.  
   - The `enable.auto.commit` property is set to `true` by default.  
   - Offset commits happen periodically based on `auto.commit.interval.ms`.  

2. **What Can Go Wrong?**  
   - **Data loss**: If our consumer crashes after processing the batch but before the auto-commit interval, Kafka thinks the batch is consumed, and we lose those messages.  
   - **Duplicate processing**: If a crash happens after processing but before the offset commit, the next consumer run will reprocess that same batch.  
   - **Lack of control**: we can’t ensure a message is truly processed (e.g., written to a database) before Kafka commits the offset.

3. **Why Use Manual Acknowledgment?**  
   - **Accuracy**: we control exactly when Kafka marks a message as processed, avoiding both data loss and duplicates.  
   - **Error handling**: If an error happens, we don’t commit offsets, ensuring the message gets retried.  
   - **Granular control**: Acknowledge messages after processing each record rather than the whole batch.
