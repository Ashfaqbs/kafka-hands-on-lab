from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'python-consumer',  # Consumer group ID
    'auto.offset.reset': 'earliest'  # Start reading from the earliest message
}

# Create the Consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
topic = 'my-topic'
consumer.subscribe([topic])

# Poll for messages
try:
    while True:
        msg = consumer.poll(1.0)  # Timeout of 1 second
        if msg is None:
            # No message received
            print("Waiting for messages...")
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition reached {msg.partition}, offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Valid message received
            print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    # Close the consumer gracefully
    consumer.close()
