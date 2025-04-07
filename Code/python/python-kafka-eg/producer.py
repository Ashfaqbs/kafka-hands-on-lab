from confluent_kafka import Producer
import time

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Address of the Kafka broker
    'client.id': 'python-producer'
}

# Create the Producer instance
# connection 
producer = Producer(conf) 

# Define the delivery report callback
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Produce messages to the Kafka topic
topic = 'my-topic'

for i in range(10):
    message = f"Message {i}"  # Message to send
    print(f"Sending: {message}")
    producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
    producer.poll(1)
# ensures that any message delivery events are processed within a 1-second timeout.

# Wait for any outstanding messages to be delivered and delivery reports to be received
# force the producer to send any remaining messages to the Kafka broker and wait until they're successfully acknowledged, or a failure occurs.
producer.flush()
