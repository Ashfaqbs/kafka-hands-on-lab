# Exploring Apache Kafka and Its Core Concepts

## Apache Kafka in Layman's Terms

### Imagine a Postal Service for Data

Kafka is like a high-speed postal service, but for data instead of physical letters. It helps different parts of a computer system send and receive messages quickly and reliably, just like a postal service helps people send and receive letters.

### Data Streams

In the world of Kafka, data comes in the form of streams, like rivers of information. These streams can contain anything from simple text to complex data like pictures or documents. These data streams flow from one place to another inside a computer system.

### Topics

Kafka organizes data streams into "topics." Topics are like categories for data streams. For example, you can have a "Weather Data" topic, a "Sales Data" topic, and so on. Each topic holds related data streams.

### Producers and Consumers

- **Producers** are like senders. They create and send data to Kafka topics. Think of them as people who put letters into mailboxes.
  
- **Consumers** are like receivers. They read and use data from Kafka topics. They're like people who take letters out of mailboxes and do something with them.

### Publish and Subscribe

Kafka follows a "publish and subscribe" model. Producers publish data to topics, and consumers subscribe to topics they are interested in. This way, consumers only get the data they care about, just like subscribing to a magazine for articles you want to read.

### Scalable and Reliable

Kafka is built to handle lots of data and keep it safe. It can handle data from many sources and make sure it gets to the right consumers, even if some parts of the system have problems.

### Real-Time Data Processing

One of Kafka's superpowers is that it enables real-time data processing. Imagine analyzing stock market data as it comes in or updating a dashboard with live information. Kafka makes this possible.

## Using Kafka with Spring Boot to Solve Real-Life Problems

Now, let's talk about how you can use Kafka with Spring Boot to solve real-life problems:

- **Real-Time Analytics**: You can use Kafka to collect data from different sources (e.g., website logs, IoT devices) and analyze it in real-time. This is great for understanding user behavior, monitoring systems, or making data-driven decisions.

- **Event-Driven Microservices**: Kafka helps in building microservices that communicate through events. When something important happens in one microservice (like a new order), it can send an event to Kafka, and other microservices can react to it. This makes your system flexible and responsive.

- **Data Integration**: Kafka can act as a bridge between different systems. For example, it can connect your e-commerce website to your inventory management system, ensuring that product availability is always up-to-date.

- **Log Aggregation**: Kafka can collect logs and error messages from various parts of your application. This makes it easier to troubleshoot issues and track what's happening in your system.

- **IoT Data Processing**: If you have IoT devices sending sensor data, Kafka can handle the massive influx of data and allow you to process it in real-time. For example, monitoring temperature changes in a warehouse.

- **Message Queues**: Kafka can serve as a robust message queue, ensuring that messages are delivered reliably between different parts of your application.

In Spring Boot, you can use the Spring Kafka library to integrate Kafka into your application. Spring Boot simplifies the setup and provides convenient abstractions for producing and consuming Kafka messages. This way, you can leverage Kafka's power without diving deep into its complexities.

By combining Kafka with Spring Boot, you can create powerful, real-time, and scalable applications that solve a wide range of real-life problems, from data analytics to event-driven microservices.



- **Leader**: The broker responsible for all reads and writes of a partition. It handles requests from producers (writing messages) and consumers (reading messages).
- **Followers**: Replicate the data from the leader to ensure redundancy and fault tolerance. If the leader fails, one of the followers can be promoted to become the new leader.

![image](https://github.com/Ashfaqbs/Springboot-Kafka-Integration/assets/105435085/fd2a146a-9739-44c1-adfb-6b08e8636fec)


0: No acknowledgment.
1 (default): Wait for acknowledgment from the leader.
all or -1: Wait for acknowledgments from the leader and all followers.












# Spring Boot Kafka Integration

**Table of Contents**
- [Introduction](#introduction)
- [Project Structure](#project-structure)
- [Kafka in Layman's Terms](#kafka-in-laymans-terms)
- [Configuration](#configuration)
- [Usage](#usage)

## Introduction

This repository demonstrates how to integrate Apache Kafka with a Spring Boot application. We'll explain the concepts and provide code examples for producing and consuming messages using Kafka topics.

## Project Structure

### SpringbootKafkaTutorialApplication (Main Application)

- `SpringbootKafkaTutorialApplication.java`: The main class that starts the Spring Boot application.

### Configuration

- `KafkaTopicConfig.java`: Configures Kafka topics using the `NewTopic` bean.

### Controllers

- `JsonKafkaController.java`: A controller that sends JSON messages to a Kafka topic.
- `MessageController.java`: A controller that sends simple text messages to a Kafka topic.

### Kafka Consumers

- `JSONKafkaConsumer.java`: A Kafka consumer that listens to a topic named "myCustomTopic1JSON" and processes JSON messages.
- `KafkaConsumer.java`: A Kafka consumer that listens to a topic named "myCustomTopic1" and processes text messages.

### Kafka Producers

- `KafkaJsonProducer.java`: A Kafka producer that sends JSON messages to the "myCustomTopic1JSON" topic.
- `KafkaProducer.java`: A Kafka producer that sends text messages to the "myCustomTopic1" topic.

### Model

- `User.java`: A simple model class used for sending JSON messages.

## Kafka in Layman's Terms

Kafka is like a messaging system that allows different parts of a software system to communicate with each other by sending and receiving messages. It's like a postal service for software, where messages are sent from one place to another.

In our example, we have a Spring Boot application that uses Kafka to send and receive messages. Messages are like letters, and Kafka ensures that messages are delivered reliably, even if some parts of the system are busy or temporarily unavailable.

## Configuration (application.properties)

```properties
# Consumer Configuration
spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=myGroup
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.spring.json.trusted.packages=*




# Producer Configuration
spring.kafka.producer.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer

Spring.kafka.producer.acks=all 
Default value is one 
Spring.kafka.producer.acks=1

0: No acknowledgment.
1 (default): Wait for acknowledgment from the leader.
all or -1: Wait for acknowledgments from the leader and all followers.
![image](https://github.com/Ashfaqbs/Springboot-Kafka-Integration/assets/105435085/2588eea6-4807-4a84-950a-1b04b0cda775)


```

### Usage
- Start your Kafka broker on localhost:9092.
- Run the Spring Boot application SpringbootKafkaTutorialApplication.java.
- Use the provided controllers to send messages to Kafka topics:
- POST /api/v1/kafka/publish for text messages.
- POST /api/v1/kafka/publishJsonMessage for JSON messages.
- The Kafka consumers will process the messages and log the results.




## Listener :

The Kafka listener (`@KafkaListener`) is a Spring abstraction that simplifies the process of consuming messages from Kafka topics. Since you already know about Kafka producers, consumers, and the core components like topics, partitions, and offsets, let me break down what the listener does in relation to these concepts.

### What is a Kafka Listener?

A Kafka listener in Spring Boot acts like an automated Kafka consumer. It listens to one or more Kafka topics for new messages and processes them. Instead of manually coding a consumer that pulls messages from Kafka, the `@KafkaListener` annotation handles that for you.

### How It Works:
- **Listener as a Consumer**: The listener is essentially a Kafka consumer under the hood. It is always "listening" to the topics you've specified and processes the messages when they arrive.
  
- **Automated Polling**: Kafka consumers usually have to poll the Kafka cluster to get messages. The listener abstracts this polling mechanism, so you don't have to manually implement the logic of consuming messages.

- **Message Processing**: When a message is consumed, the method annotated with `@KafkaListener` is triggered, and the message is passed to that method for further processing. In your case, you're consuming messages as `User` objects.

### Example in Context:

The code you shared:
```java
@KafkaListener(topics = "${app.topic.name}", groupId = "my-group")
public void consumeEvents(User user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, 
                          @Header(KafkaHeaders.OFFSET) long offset) {
    log.info("Received: {} from {} offset {}", user, topic, offset);
    // process the message
}
```

- **`@KafkaListener`**: This annotation defines that this method is a Kafka listener, meaning it will automatically consume messages from the topic specified.
- **`topics`**: Defines which Kafka topics the listener should listen to.
- **`groupId`**: Assigns the listener to a specific consumer group (so that it works as part of a Kafka consumer group).
- **`User user`**: The message consumed is deserialized into a `User` object (based on the configured Kafka serializer/deserializer).
- **`@Header`**: You can retrieve metadata such as topic name, offset, partition, etc., by accessing the message headers.

### Why Use a Listener?
- **Simplifies Consumer Logic**: It abstracts the complexity of consuming messages, handling offsets, and interacting with the Kafka cluster.
- **Spring Integration**: It integrates well with other Spring features like dependency injection, transactional support, and error handling.
- **Concurrency**: You can configure concurrency on listeners to consume messages from multiple partitions in parallel.

So, in short, a Kafka listener simplifies the process of consuming messages and automatically processes them when they arrive. It handles polling, deserialization, and other boilerplate logic behind the scenes.

## Kafka Listener in Kafka's Own Context
In Kafka, a **listener** typically refers to how Kafka brokers communicate with other Kafka components. It's more related to **listeners for network communication**, such as how brokers listen to client requests from producers or consumers. Here’s where listeners play a role in Kafka:

1. **Broker Listeners**: Kafka brokers use listeners to allow connections from clients (producers, consumers, admin clients). A listener is tied to a specific port (like `PLAINTEXT://localhost:9092`) and allows Kafka clients to send and receive data.

2. **Consumer Failures (Listeners Going Down)**: When people say "listeners are down" in the context of Kafka, they are usually talking about consumer listeners not receiving messages. This can happen for various reasons on the **consumer** side.

### Reasons Why Kafka Consumers (Listeners) Go Down

1. **Consumer Group Rebalancing**: Kafka uses consumer groups to ensure each partition is consumed by only one consumer at a time. If a consumer fails or leaves, Kafka rebalances the group to redistribute the partitions to other consumers. During this process, it may seem like the "listener is down" because consumption pauses until the rebalance completes.

2. **Network Issues**: Kafka consumers need to regularly send heartbeats to the broker to keep their session alive. If there is network lag or interruption between the consumer and the broker, the broker may consider the consumer dead and rebalance the partitions. This could lead to delays in consumption.

3. **Partition Assignment Failures**: If consumers fail to get assigned partitions correctly (due to improper configuration or issues in the cluster), they may not be able to consume messages. This would give the impression that the listener is down.

4. **Offset Commit Failures**: Kafka consumers commit offsets to track how much of the topic they've processed. If consumers fail to commit offsets due to network problems or broker issues, they might stop consuming new messages.

5. **Zookeeper or Broker Issues**: Kafka brokers and Zookeeper (if you're using Kafka 2.x or lower) must be healthy for consumers to receive messages. If there's an issue with the broker or Zookeeper, consumers might be disconnected or unable to pull messages from the topic.

6. **Consumer Lag**: If the consumer is slow to process messages (due to resource constraints, slow deserialization, or downstream processing issues), it may appear as if the consumer is "down" because it's not consuming as fast as the messages are being produced.

7. **Listener Configuration in Kafka Brokers**: At the broker level, Kafka listeners need to be configured correctly for clients to connect. A misconfigured listener or port can cause consumers not to be able to connect, which can make it seem like the consumers are down.

### How to Troubleshoot "Listeners Are Down"
1. **Check Consumer Logs**: Look for errors related to consumer group rebalancing, network issues, or partition assignment.
2. **Monitor Consumer Lag**: Tools like Kafka’s native `kafka-consumer-groups.sh` or third-party monitoring tools (e.g., Confluent Control Center, Grafana with Kafka metrics) can help you monitor lag and identify if consumers are falling behind.
3. **Ensure Broker and Zookeeper Health**: Use Kafka and Zookeeper monitoring tools to check the health of brokers and Zookeeper nodes.
4. **Heartbeat Timeouts**: If consumers are experiencing heartbeat timeouts, check the network connection and adjust Kafka configurations like `session.timeout.ms` and `heartbeat.interval.ms`.

In summary, when Kafka "listeners are down," it's often about consumer-side failures (like network issues, consumer rebalancing, or configuration problems) or Kafka broker-side listener configuration issues. These are crucial for maintaining the availability and reliability of Kafka consumers.
