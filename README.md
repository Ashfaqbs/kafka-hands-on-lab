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

```

### Usage
- Start your Kafka broker on localhost:9092.
- Run the Spring Boot application SpringbootKafkaTutorialApplication.java.
- Use the provided controllers to send messages to Kafka topics:
- POST /api/v1/kafka/publish for text messages.
- POST /api/v1/kafka/publishJsonMessage for JSON messages.
- The Kafka consumers will process the messages and log the results.
