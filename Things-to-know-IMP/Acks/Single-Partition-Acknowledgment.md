Single Partition Acknowledgment:

Producer sends data to a Kafka topic with one partition.

Consumer reads data from the topic and processes it.

Offset tracking: Kafka maintains the offset for that partition, and after the consumer successfully processes a message, it acknowledges the offset.

If an error occurs, the consumer will stop at the last acknowledged offset and retry from there.
