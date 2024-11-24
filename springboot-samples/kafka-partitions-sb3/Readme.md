The main purposes of **partitions** in Kafka are:

### 1. **Load Balancing**
   - **Distribute Data Across Brokers**: In Kafka, a **topic** can have multiple partitions. This allows Kafka to distribute the data across different brokers in the cluster. If you have more than one partition, data will be spread across multiple brokers, helping to **balance the load** and prevent any single broker from being overwhelmed with too much data.
   - **Even Distribution**: Each producer sends data to the available partitions, and if no key is provided, Kafka uses a round-robin strategy to distribute messages evenly across the partitions. This ensures no single partition gets too much data while others are under-utilized.

### 2. **Parallelism**
   - **Consumer Parallelism**: Partitions enable **parallel consumption**. Kafka allows multiple consumers in a consumer group to read from different partitions at the same time, allowing them to process messages concurrently. Each partition can be consumed by **only one consumer in a group** at a time, but with multiple partitions, multiple consumers can read from different partitions simultaneously, improving throughput and speed.
   - **Scalability**: The more partitions you have, the more consumers can be added to scale out the processing of messages. Each partition can be processed in parallel by separate consumers in the same consumer group or across different consumer groups.

### 3. **Fault Tolerance**
   - **Replication**: Partitions also contribute to **fault tolerance** in Kafka. Each partition can be **replicated** across multiple brokers. So, if one broker goes down, Kafka can still serve data from the replica of that partition, ensuring no data loss and continuous availability.
   - **Consumer Failover**: In case a consumer fails, another consumer can take over and start consuming messages from the partition it was previously consuming.

### Key Benefits of Partitions:
1. **Improved Throughput**: More partitions allow Kafka to handle more messages, as multiple consumers can process them in parallel.
2. **Scalability**: Kafka scales horizontally by adding more partitions, enabling it to handle larger volumes of data.
3. **Consumer Load Distribution**: Multiple consumers can be assigned to different partitions in a consumer group, distributing the load and avoiding a single consumer from being overwhelmed with too much data.
4. **Order Within Partitions**: Kafka guarantees message order **within a partition**, but not across partitions. If order is important for your use case, you would control which partition the message goes to using the **key**.

---

### Summary of Why Partitions Are Important:
- **Load balancing** across brokers.
- **Parallel processing** of data by consumers, improving throughput and speed.
- **Scalability**: As the data volume increases, adding partitions allows Kafka to scale without performance degradation.
- **Fault tolerance** and **data replication** for high availability.




## Observations :

```
Bean
	public NewTopic myTopic() {
		return TopicBuilder.name("my-topic").partitions(3) // Configure 3 partitions
				.replicas(1) // Number of replicas (adjust as per your setup)
				.build();
	}
	
```

We have 3 partitions namely :  

group2: partitions assigned: [my-topic-0, my-topic-1, my-topic-2]
group1: partitions assigned: [my-topic-0, my-topic-1, my-topic-2]
got this from logs 

and when called the api :

```
C:\Users\ashfa>curl http://localhost:8080/api/kafka/publish?message=Message1
Message sent: Message1
C:\Users\ashfa>curl http://localhost:8080/api/kafka/publish?message=Message1
Message sent: Message1
C:\Users\ashfa>curl http://localhost:8080/api/kafka/publish?message=Message2
Message sent: Message2
C:\Users\ashfa>curl http://localhost:8080/api/kafka/publish?message=Message3
Message sent: Message3
C:\Users\ashfa>
```

Logs :

```
group1 Received message: Message1
group1 Topic: my-topic
group2 Received message: Message1
group1 Partition: 0
group2 Topic: my-topic
group1 Offset: 30
group2 Partition: 0
group2 Offset: 30
group1 Received message: Message2
group1 Topic: my-topic
group2 Received message: Message2
group2 Topic: my-topic
group2 Partition: 0
group2 Offset: 31
group1 Partition: 0
group1 Offset: 31
group1 Received message: Message3
group1 Topic: my-topic
group1 Partition: 0
group1 Offset: 32
group2 Received message: Message3
group2 Topic: my-topic
group2 Partition: 0
group2 Offset: 32


```

## Why only one Partition was in action and rest 2 not :
 The current behavior shows that **both consumer groups are independently consuming the same data** from the same partition (`Partition: 0`). This happens because Kafka assigns partitions to consumers within a group based on:

1. **Number of partitions**.
2. **Number of consumers in a consumer group**.

Let me clarify the key points:

---

### **Why Is Only One Partition Being Used?**
1. **Low Volume of Data**:
   - Since you're publishing a small number of messages (`Message1`, `Message2`, `Message3`), Kafka is routing all messages to **one partition** (in this case, Partition 0). This behavior is common when the producer doesn't specify a key for partitioning. Kafka then defaults to a **round-robin** or sequential assignment.

2. **Single Consumer per Group**:
   - Both **Group 1** and **Group 2** have **one consumer each**. Within a consumer group:
     - If there are fewer consumers than partitions, some partitions will remain unassigned.
     - Here, since the data volume is small and only one consumer exists in each group, the single consumer in each group is handling Partition 0.

---

### **What Happens if Data Volume Increases?**
If the volume of data increases significantly, all partitions may start being used. Here's why:

- When messages are produced in high volume:
  - Kafka’s default **partitioning strategy** starts spreading the messages across partitions to balance the load.
  - Each partition will maintain its own sequence of messages, but messages will be distributed across all partitions for throughput.

---

### **What Happens with Multiple Consumers in the Same Group?**
If you add more consumers **within a single consumer group**, Kafka will **rebalance** the partitions among the consumers. For example:

- If you have 3 partitions and 3 consumers in **Group 1**:
  - Each consumer will be assigned **1 partition**.
  - Messages from different partitions will be processed **in parallel** by different consumers.

For **Group 2**, since it’s independent, the same behavior will apply—it will still consume all messages from all partitions assigned to its consumers.

---

### **How to Test This?**
To see all partitions being used:

1. **Send High-Volume Data**:
   Use a loop to send many messages (e.g., 100 or more). For example:
   ```bash
   for i in {1..100}; do curl "http://localhost:8080/api/kafka/publish?message=Message$i"; done
   ```

2. **Check Logs**:
   - You should see messages being assigned to **different partitions**.
   - Kafka’s **round-robin distribution** will spread the messages across partitions evenly (assuming no key is specified).

3. **Add More Consumers to a Group**:
   - Add additional consumers to **Group 1** and observe how the partitions are distributed among them.

---

### **Key Insights**
- Kafka uses **partitions** to enable parallelism and scalability.
- A **partition is processed by only one consumer** within the same group to avoid duplicates.
- **Multiple consumer groups** will independently consume the same data from all partitions.


## Control the partions with keys :

API Call :

```

curl "http://localhost:8080/api/kafka/publishWithKey?key=key1&message=HelloPartition1"
curl "http://localhost:8080/api/kafka/publishWithKey?key=key2&message=HelloPartition2"
curl "http://localhost:8080/api/kafka/publishWithKey?key=key1&message=AnotherMessageForPartition1"

```
note : the working of actual is diff then api call telling this key will be partiton1 or 2 , the formula is there , that OP number the partion is decided

logs result :
```
group2 Received message: HelloPartition1
group1 Received message: HelloPartition1
group2 Topic: my-topic
group1 Topic: my-topic
group2 Partition: 2
group1 Partition: 2
group2 Offset: 0
group1 Offset: 0
group2 Received message: HelloPartition2
group2 Topic: my-topic
group2 Partition: 2
group2 Offset: 1
group1 Received message: HelloPartition2
group1 Topic: my-topic
group1 Partition: 2
group1 Offset: 1
group2 Received message: AnotherMessageForPartition1
group2 Topic: my-topic
group2 Partition: 2
group2 Offset: 2
group1 Received message: AnotherMessageForPartition1
group1 Topic: my-topic
group1 Partition: 2
group1 Offset: 2
group1 Received message: AnotherMessageForPartition1
group1 Topic: my-topic
group1 Partition: 1
group1 Offset: 1
group2 Received message: AnotherMessageForPartition1
group2 Topic: my-topic
group2 Partition: 1
group2 Offset: 1
group2 Received message: HelloPartition3
group2 Topic: my-topic
group2 Partition: 1
group2 Offset: 2
group1 Received message: HelloPartition3
group1 Topic: my-topic
group1 Partition: 1
group1 Offset: 2

```

## Observation :

im seeing something off , if the key is 1 or 2 is Partition: 2 and if the key is 0 its Partition: 1 why man 

This behavior is entirely based on Kafka's **partitioning strategy** when using a key. Let’s break it down:

---

### **Why Are Keys Always Targeting Certain Partitions?**
Kafka uses a **hashing algorithm** to determine the partition. Here's the process:

1. **Key Hashing**: 
   - Kafka applies a **hashing function** (like `Murmur2` in the default setup) to the key (`key1`, `key2`, etc.).
   - Example: `hash(key1)` produces a unique hash value for `key1`.

2. **Modulo Operation**: 
   - The hash value is divided by the number of partitions (`n`) using the modulo operator (`%`).
   - Formula:  
     ```
     partition = hash(key) % number_of_partitions
     ```
   - This determines the specific partition where the message is routed.

---

### **Understanding Your Logs**
You have **two partitions** (`1` and `2`) for the topic **my-topic**. Here's why you're seeing this behavior:

1. **For `key1` and `key2`**:
   - Both keys produce hashes that, when modded by the partition count (`2`), resolve to **Partition 2**.

2. **For `key0`**:
   - The hash of `key0` modded by `2` resolves to **Partition 1**.

The **hashing is deterministic**, meaning:
- The same key will always hash to the same partition.
- Different keys can also hash to the same partition if their hash values align in the modulo operation.

---

### **Why Does This Matter?**
1. **Consistency**:
   - All messages with the same key are guaranteed to land in the same partition, ensuring order is preserved for that key.

2. **Logical Grouping**:
   - For example, all data related to "Customer 1" (key = `cust1`) will always go to a specific partition, ensuring consistent processing.

3. **Balanced Distribution**:
   - If keys are uniformly distributed, partitions will be well-balanced in terms of data. However, if some keys are more frequent, those partitions may become hotspots.

---

### **How to Validate the Partition Logic?**
To further test, you can:
1. Add **more partitions** and observe how the keys map to them.
2. Use different keys to see how the partition assignment changes.

For example:
```bash
curl "http://localhost:8080/api/kafka/publishWithKey?key=key3&message=HelloPartition3"
curl "http://localhost:8080/api/kafka/publishWithKey?key=key4&message=HelloPartition4"
```

### **Visualizing the Partition Formula**
With `n=2` partitions:
| Key      | Hash(Key) | Hash(Key) % 2 | Partition |
|----------|-----------|---------------|-----------|
| `key0`   | Hash(A)   | 1             | 1         |
| `key1`   | Hash(B)   | 0             | 2         |
| `key2`   | Hash(C)   | 0             | 2         |
| `key3`   | Hash(D)   | 1             | 1         |

---

### **What If You Want Custom Partitioning?**
You can implement a **custom partitioner** in Kafka to override the default hash-based behavior.



