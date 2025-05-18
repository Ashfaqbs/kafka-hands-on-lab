## 🧠 Kafka + ZooKeeper Architecture (pre-KRaft mode)

> Kafka (before version 2.8 / 3.x) used **ZooKeeper** for managing metadata and cluster coordination.

---

### 🧩 What ZooKeeper Does in Kafka:

* Tracks the **status of Kafka brokers (servers)**.
* Manages **leader election** for partitions.
* Maintains the **cluster metadata** (e.g., what topics, partitions, and replicas exist, and where they are).

---

### ✅ Our Understanding:

We said:

> "If I have 3 bootstrap servers, the ZooKeeper checks if they're alive by receiving heartbeats. If 2 are sending heartbeat and 1 is not, ZooKeeper considers the 3rd dead and data is not sent to that server."

That’s **very close to accurate**, just with a few clarifications needed:

---

### 🔄 Heartbeats and Broker Liveness:

* **Kafka brokers** (servers) send **heartbeat signals** to **ZooKeeper** at regular intervals.
* If a broker **misses multiple heartbeats** (typically for a configurable timeout period), ZooKeeper assumes that broker is **dead or disconnected**.

#### 🛑 What Happens Then?

1. **ZooKeeper marks the broker as dead**.
2. **Partition leadership is re-elected** — partitions that were led by that broker will get **new leaders** on healthy brokers.
3. **Producers and consumers stop communicating** with the dead broker.
4. **Data will NOT be sent** to the dead broker until it rejoins the cluster and catches up.

> ✅ So yes — if Broker 3 stops heartbeating, ZooKeeper declares it dead, and Kafka won’t route data to it.

---

### ⚙️ What about replication?

If Broker 3 held **replicas** of some partitions:

* Those replicas are marked as **out-of-sync** or **offline**.
* Kafka continues with the **remaining in-sync replicas (ISR)**.
* If replication factor was 3, but only 2 brokers are alive, **Kafka runs with 2 replicas** for now.

---

### 🔧 Example:

We have:

* **3 Kafka brokers**: Broker 1, 2, 3
* **ZooKeeper** running

#### Scenario:

* Broker 3 crashes or network partitions it.
* It stops sending heartbeats.
* After timeout (e.g., 6 seconds default), ZooKeeper **removes it from the list of live brokers**.
* Kafka **reassigns leaders** for any partitions where Broker 3 was the leader.
* Broker 3 is now excluded from data traffic until it rejoins.

---

### 🆕 Kafka Without ZooKeeper (KRaft mode)

As of Kafka 2.8+ (official in 3.x), Kafka started removing the dependency on ZooKeeper with **KRaft (Kafka Raft)** mode, where:

* Kafka manages its own metadata quorum (no ZooKeeper).
* Brokers communicate with **controller nodes** directly for health checks and leader election.

> But our explanation is still 100% valid for **ZooKeeper-based Kafka**, which is still widely used.

---

### ✅ Summary

| Concept                              | Behavior |
| ------------------------------------ | -------- |
| Brokers send heartbeats to ZK        | ✅ Yes    |
| If heartbeat stops → broker is dead  | ✅ Yes    |
| ZooKeeper triggers leadership change | ✅ Yes    |
| Data stops flowing to that broker    | ✅ Yes    |
| Broker must rejoin to be used again  | ✅ Yes    |

---
