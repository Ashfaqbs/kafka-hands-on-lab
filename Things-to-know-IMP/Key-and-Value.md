In Kafka, we define:  
- **Key & Value Serializer on Producer Side** → Converts objects into byte arrays before sending them.  
- **Key & Value Deserializer on Consumer Side** → Converts byte arrays back into objects after receiving them.  

---

### **1. Why Does Kafka Use a Key?**  
The **key** is primarily used for **partitioning**. When a message is sent to a Kafka topic, the partitioning logic works as follows:  
- **If a key is provided** → Kafka **hashes the key** and determines which partition the record will be sent to.  
- **If no key is provided** → Kafka distributes messages **randomly** (round-robin) across partitions.

### **2. Importance of Key in Kafka**  
The key is crucial for:  
✅ **Ensuring Order Within a Partition** → Messages with the same key always go to the same partition, maintaining order.  
✅ **Load Balancing Across Partitions** → Helps distribute messages logically.  
✅ **Efficient Processing in Consumers** → If consumers process events **based on a specific key**, keeping keys consistent ensures related messages are processed together.  

For example:  
- **E-commerce Orders**: If the key is `customerId`, all orders from the same customer go to the same partition.  
- **IoT Data Processing**: If the key is `deviceId`, all messages from a specific device land in the same partition, allowing efficient analysis.  

---

### **3. Can the Key Be Something Other Than String?**  
Yes! The **key** can be any serializable Java object. It’s usually a `String`, but it can be:  
- **UUID** → For unique tracking.  
- **Integer/Long** → For numeric identifiers.  
- **Custom Objects** → If a custom object is used, a custom serializer/deserializer is needed.  

**Example:** If you want to use a `UUID` as a key, you can use `org.apache.kafka.common.serialization.UUIDSerializer`.  

---

### **4. Code Example (Sending and Receiving Key & Value in Kafka)**  

#### **Producer (Sending Key & Value)**
```java
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaKeyValueProducer {
    public static void main(String[] args) {
        // Kafka Properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "orders";
        String key = "customer123";  // Key: Same customer goes to same partition
        String value = "Order ID: 98765, Amount: 1500";

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.println("Sent: Key=" + key + ", Value=" + value + 
                                   " to Partition=" + metadata.partition());
            } else {
                exception.printStackTrace();
            }
        });

        producer.close();
    }
}
```

#### **Consumer (Receiving Key & Value)**
- Note pool() is expired but  the same goes for @eventListener springboot code

```java
import org.apache.kafka.clients.consumer.*;
import java.util.Collections;
import java.util.Properties;

public class KafkaKeyValueConsumer {
    public static void main(String[] args) {
        // Kafka Properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("orders"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Received: Key=" + record.key() + 
                                   ", Value=" + record.value() + 
                                   ", Partition=" + record.partition());
            }
        }
    }
}
```

---

### **5. Observing the Key’s Impact**  
If you run the above code multiple times with different keys:  
- **Same Key → Same Partition**  
- **Different Keys → Distributed Across Partitions**  

For example:  
| **Key**          | **Value**                     | **Partition** |
|-----------------|-----------------------------|--------------|
| `"customer123"` | `"Order ID: 98765"`         | `Partition 1` |
| `"customer123"` | `"Order ID: 98766"`         | `Partition 1` |
| `"customer456"` | `"Order ID: 55555"`         | `Partition 2` |
| `"customer789"` | `"Order ID: 33333"`         | `Partition 3` |

---

### **6. When to Use a Key vs. When Not To?**  
✅ **Use a Key When:**  
- You want to **group related messages** in the same partition (e.g., all transactions for the same user).  
- Order **must be preserved for a specific entity**.  
- Load balancing should be controlled across partitions.  

❌ **Do Not Use a Key When:**  
- You want Kafka to randomly distribute messages **for better parallelism**.  
- The order of processing **does not matter**.  

---

### **7. Key Takeaways**  
✔ **Key is crucial for partitioning** in Kafka.  
✔ It ensures **order** within a partition and **logical grouping** of messages.  
✔ It can be **anything serializable** (String, UUID, Integer, Custom Object).  
✔ If **no key is provided**, Kafka distributes messages randomly across partitions.  
✔ **Consumer can read the key**, which helps in processing messages efficiently.  

---
