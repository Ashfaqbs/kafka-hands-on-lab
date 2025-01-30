### **What is Event-Driven Architecture?**
Event-Driven Architecture (EDA) is a software design pattern in which services or components communicate asynchronously by producing and consuming **events**. An **event** is a significant state change in the system, such as **"Order Placed,"** **"Payment Processed,"** or **"User Registered."**  

EDA decouples producers and consumers, allowing flexibility, scalability, and real-time processing.

---

### **How Kafka is Event-Driven?**
Kafka is an event-driven system because:
1. **Producers Publish Events** → Kafka producers send messages (events) to a topic.
2. **Kafka Stores Events** → The topic holds events in a log-based structure.
3. **Consumers Subscribe to Events** → Consumers (services) listen for new messages and process them asynchronously.

Kafka **ensures loose coupling**, meaning producers don't know about consumers, and multiple consumers can process the same event for different purposes (e.g., one consumer updates a database, another sends a notification).

---

### **When to Use Event-Driven Architecture?**
1. **Asynchronous Communication** → Microservices can operate independently without waiting for responses.
2. **Scalability** → Multiple consumers can scale horizontally for increased load.
3. **Real-Time Processing** → Instant updates (e.g., stock price updates, order tracking).
4. **Decoupling Services** → Producers and consumers can evolve independently.

---

### **Spring Boot Kafka Event-Driven Example**
Here’s a simple Spring Boot project using Kafka:

#### **1️⃣ application.properties**
Define Kafka properties.
```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.group-id=my-group
```

---

#### **2️⃣ Producer - Sending Events**
```java
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message) {
        kafkaTemplate.send("order-topic", message);
        System.out.println("Produced event: " + message);
    }
}
```

---

#### **3️⃣ Controller - Exposing API to Send Events**
```java
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {
    private final KafkaProducer kafkaProducer;

    public KafkaController(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/send/{message}")
    public String sendMessage(@PathVariable String message) {
        kafkaProducer.sendMessage(message);
        return "Message Sent: " + message;
    }
}
```

---

#### **4️⃣ Consumer - Listening & Processing Events**
```java
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    @KafkaListener(topics = "order-topic", groupId = "my-group")
    public void listen(ConsumerRecord<String, String> record) {
        System.out.println("Consumed event: " + record.value());
        processOrder(record.value());
    }

    private void processOrder(String order) {
        System.out.println("Processing order: " + order);
        // Simulate processing logic (e.g., saving to DB)
    }
}
```

---

### **📌 How Event-Driven Works Here?**
1. **User Calls API** → `POST /api/kafka/send/{message}`
2. **Producer Publishes Event** → Sends message to Kafka topic (`order-topic`).
3. **Kafka Stores the Event** → Retains message for consumers.
4. **Consumer Listens & Processes** → Receives event, logs it, and simulates order processing.

---

### **🔹 When is Event-Driven Architecture Useful?**
- **Order Processing System**: Order service emits an "order placed" event; multiple consumers handle stock updates, payment, notifications.
- **User Registration**: "User Signed Up" event triggers email confirmation and profile creation.
- **IoT & Real-Time Data**: Devices continuously publish sensor data; consumers process alerts.

This approach **decouples components**, allowing independent scaling and asynchronous communication.

---
