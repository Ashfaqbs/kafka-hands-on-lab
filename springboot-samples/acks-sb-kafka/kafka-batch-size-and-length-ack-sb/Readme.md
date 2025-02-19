Below is a detailed explanation of how the batch-mode consumer works—with both quantity-based and size-based acknowledgment logic—when our API sends a single message at a time. We'll walk through the configuration, the role of the container factory bean, and the flow of data from the API to Kafka and then to the consumer.

---

## Overall Flow

1. **API and Producer:**  
   - our REST API (via `KafkaController`) receives a call such as:  
     ```bash
     curl http://localhost:8080/api/send
     ```
   - The controller calls `KafkaProducerService.sendMessage("my-topic", message)`.
   - The producer (using `KafkaTemplate`) sends the single message (e.g., `"Hello Kafka"`) to the Kafka topic `"my-topic"`.

2. **Kafka Storage:**  
   - Kafka writes the message to the topic. Even though ou’re sending one message per API call, over time (or via rapid calls), multiple messages will accumulate in the topic.

3. **Consumer Polling in Batch Mode:**  
   - our consumer is configured to operate in batch mode. That means every time it polls Kafka, it gathers all available messages into a **List**.
   - Even if only one message is available, it will appear as a list with one element.
   - The container factory (named `"batchFactory"`) is used to create a listener container with custom settings—batch mode enabled and manual acknowledgment mode.

4. **Processing and Acknowledgment:**  
   - Once the consumer receives a batch, it processes the list of messages.
   - Depending on our business logic, we can choose to acknowledge the batch either when a certain number of messages have been processed (quantity-based) or when the cumulative size of the messages reaches a threshold (size-based).
   - When we call `acknowledgment.acknowledge()`, the consumer commits the offsets for the entire batch, so those messages are not reprocessed.

---

## Configuration Details

### Consumer Configuration with Batch Mode

we define a custom listener container factory (for example, in a class called `KafkaBatchConsumerConfig`) like this:

```java
@Configuration
public class KafkaBatchConsumerConfig {

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Disable auto-commit so that we can use manual acknowledgment
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "batchFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // Enable batch listening so that the listener receives a List<String>
        factory.setBatchListener(true);
        // Set manual acknowledgment mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }
}
```

**Why a Container Factory?**  
- The container factory is the component that creates the Kafka listener container. By defining it with a custom bean name (here, `"batchFactory"`), we can reference it in our `@KafkaListener` annotation.  
- This factory is configured to enable **batch mode** (so our listener method receives a List of messages) and to disable auto-commit (so we have full control via manual acknowledgment).

---

## Consumer Code for Batch Acknowledgment

Now, let’s look at two examples of consumer logic: one for acknowledging based on quantity (e.g., 25 messages) and one based on the cumulative size (e.g., 1 MB).

### 1. Quantity-Based Acknowledgment

In this example, the consumer processes each batch and checks if the batch size is at least 25 messages. If it is, it acknowledges the batch immediately.

```java
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class BatchConsumerByQuantity {

    private static final int MESSAGE_THRESHOLD = 25;

    @KafkaListener(topics = "my-topic", groupId = "my-group", containerFactory = "batchFactory")
    public void consume(List<String> messages, Acknowledgment acknowledgment) {
        System.out.println("Received batch of " + messages.size() + " messages.");

        // Process each message in the batch
        for (String message : messages) {
            System.out.println("Processing message: " + message);
        }

        // Check if the batch meets the quantity threshold
        if (messages.size() >= MESSAGE_THRESHOLD) {
            System.out.println("Acknowledging batch after processing " + messages.size() + " messages.");
        } else {
            System.out.println("Batch size below threshold; acknowledging anyway to commit available messages.");
        }
        // Acknowledge the batch to commit offsets
        acknowledgment.acknowledge();
    }
}
```

**Flow Explanation for Quantity-Based Acknowledgment:**
- **API Call:**  
  Each API call sends a single message. Over time, multiple messages accumulate.
- **Polling:**  
  The consumer polls Kafka and collects available messages into a List.
- **Processing:**  
  The consumer processes the list. If the list size is 25 or more, it meets our threshold and calls `acknowledgment.acknowledge()`.
- **Commit:**  
  Once acknowledged, the consumer commits the offsets, so messages already processed will not be re-read.

### 2. Size-Based Acknowledgment

Here, the consumer calculates the cumulative size (in bytes) of all messages in the batch and acknowledges when that size reaches or exceeds 1 MB.

```java
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class BatchConsumerBySize {

    // Size threshold: 1 MB (1 * 1024 * 1024 bytes)
    private static final int SIZE_THRESHOLD = 1 * 1024 * 1024;

    @KafkaListener(topics = "my-topic", groupId = "my-group", containerFactory = "batchFactory")
    public void consume(List<String> messages, Acknowledgment acknowledgment) {
        int cumulativeSize = 0;
        System.out.println("Received batch of " + messages.size() + " messages.");

        for (String message : messages) {
            System.out.println("Processing message: " + message);
            // Calculate size using UTF-8 encoding
            cumulativeSize += message.getBytes(StandardCharsets.UTF_8).length;
        }

        System.out.println("Cumulative size of batch: " + cumulativeSize + " bytes.");

        if (cumulativeSize >= SIZE_THRESHOLD) {
            System.out.println("Acknowledging batch after reaching size threshold of " + SIZE_THRESHOLD + " bytes.");
        } else {
            System.out.println("Cumulative size below threshold; acknowledging batch to commit available messages.");
        }
        // Acknowledge the batch to commit offsets
        acknowledgment.acknowledge();
    }
}
```

**Flow Explanation for Size-Based Acknowledgment:**
- **API Call:**  
  Similar to before, each API call sends one message.
- **Polling:**  
  The consumer collects available messages into a List.
- **Processing:**  
  The consumer iterates through the list, calculates the total size of messages in bytes.
- **Threshold Check:**  
  If the cumulative size reaches 1 MB, the condition is met; otherwise, it still acknowledges what it has.
- **Commit:**  
  Calling `acknowledgment.acknowledge()` commits the offsets for all messages in that batch.

---

## Recap and Key Points

- **Producer Side:**  
  our API sends single messages using the `KafkaProducerService`. This part remains unchanged.
  
- **Consumer Configuration:**  
  The custom container factory (`batchFactory`) is created with batch mode enabled and manual acknowledgment turned on. This is why our listener method receives a List of messages.

- **Batch Mode Behavior:**  
  - Even if a single message is sent, when the consumer polls, it wraps that message into a List.
  - If multiple messages are available (from repeated API calls), they will appear as a List containing all those messages.
  
- **Acknowledgment Logic:**  
  - **Quantity-Based:** Processes the list and checks if the count meets a threshold (e.g., 25 messages) before acknowledging.
  - **Size-Based:** Sums up the size of messages and acknowledges when the cumulative size exceeds a threshold (e.g., 1 MB).

- **Manual Acknowledgment:**  
  By calling `acknowledgment.acknowledge()`, we control when offsets are committed. This ensures that only messages that have been fully processed are marked as done, preventing reprocessing after failures.

---
