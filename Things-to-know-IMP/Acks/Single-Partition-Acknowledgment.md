Single Partition Acknowledgment:

Producer sends data to a Kafka topic with one partition.

Consumer reads data from the topic and processes it.

Offset tracking: Kafka maintains the offset for that partition, and after the consumer successfully processes a message, it acknowledges the offset.

If an error occurs, the consumer will stop at the last acknowledged offset and retry from there.


EG Flow

Imagine a Kafka topic with one partition.

The producer sends data to this partition. Each message gets stored at a unique offset (0, 1, 2, ...).

The consumer polls the topic and reads the messages sequentially.

Kafka tracks the consumer offset, representing the last acknowledged message.

After processing each message (or batch of messages), the consumer commits the offset to Kafka (either automatically or manually).

If the consumer fails after processing the second message but before acknowledging the third:

On restart, the consumer resumes from the last committed offset (offset 2), reprocessing any unacknowledged messages.
