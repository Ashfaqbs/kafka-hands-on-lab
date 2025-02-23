Multiple Partitions Acknowledgment:

Producer sends data to a topic with multiple partitions.

Kafka distributes data across partitions, either using a key-based strategy or round-robin.

Each partition has its own offset.

Consumer(s) track offsets for each partition independently.

On error, the consumer reprocesses from the last acknowledged offset for each partition.

Key Insight:

Acknowledgment is partition-specific and does not depend on brokers, replicas, or servers. It solely relies on offset management.
