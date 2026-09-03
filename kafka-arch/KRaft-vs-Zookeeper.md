Every runnable setup elsewhere in this repo (`docker-kafka/`, `confluent-kafka/confluent-kafka-yml/`, `Kafka-Intro.txt`) still boots Kafka the "classic" way, with a separate ZooKeeper container coordinating the brokers. That's how Kafka worked for most of its life, but it's no longer how you'd set it up today. This doc covers what changed and why, plus a manifest to try the new way.

## **Why Kafka Needed ZooKeeper in the First Place**
Kafka brokers need to agree on shared cluster state — which broker is the controller, which broker leads which partition, cluster membership, topic configs. Kafka didn't build its own consensus system for this early on, so it outsourced that job to **ZooKeeper**, a separate distributed coordination service. This worked, but it meant running and operating **two distributed systems** to get one Kafka cluster: ZooKeeper's own ensemble (its own leader election, its own quorum, its own failure modes) *plus* the Kafka brokers themselves.

## **What KRaft Actually Is**
**KRaft** (Kafka Raft — pronounced "craft") removes ZooKeeper entirely by having Kafka manage its own metadata using the **Raft consensus protocol**, built directly into Kafka. A small set of brokers take on a **controller** role (instead of being handled by ZooKeeper), and Raft handles leader election and replicating metadata (topic configs, partition assignments, ACLs) across them — the same job ZooKeeper used to do, just built into Kafka itself.

Practically, this means:
- **One system to run and monitor**, not two.
- **Faster controller failover** — no longer bottlenecked by loading the full metadata state from ZooKeeper on every controller change.
- **Higher partition-count ceilings** — ZooKeeper's design became the bottleneck for clusters with very large numbers of partitions; KRaft's metadata log scales further.
- KRaft has been production-ready since Kafka 3.3+ and is the **default for new clusters from Kafka 4.0 onward** — ZooKeeper mode is deprecated and being removed, not just "the older option."

## **Broker vs. Controller Roles**
In KRaft mode, every node runs with an explicit `process.roles`, instead of every node just being an undifferentiated "broker" with ZooKeeper handling coordination separately:
| Role | What it does |
|---|---|
| `controller` | Participates in the Raft quorum, manages cluster metadata |
| `broker` | Serves produce/consume traffic, stores partition data |
| `broker,controller` | Both — fine for dev/small clusters; production usually separates them |

## **Sample Manifest: Single-Node KRaft Cluster**
A minimal `docker-compose.yml` to get a KRaft-mode broker running locally, no ZooKeeper container needed at all — contrast this with the multi-container ZooKeeper+broker setups under `../docker-kafka/`:
```yaml
version: "3.8"
services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: kafka-kraft
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-kraft:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```
```sh
docker compose up -d
docker exec -it kafka-kraft /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-sample-topic --partitions 3 --replication-factor 1
docker exec -it kafka-kraft /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
No `KAFKA_ZOOKEEPER_CONNECT` anywhere — `KAFKA_CONTROLLER_QUORUM_VOTERS` is the KRaft equivalent, listing which node IDs form the metadata quorum (`1@kafka-kraft:9093` = node 1, reachable at `kafka-kraft:9093`, using the internal `CONTROLLER` listener). Every Spring Boot example elsewhere in this repo works against this exactly the same way — KRaft changes how the *cluster* coordinates itself, not the client-facing produce/consume API.

## **Where to Go for a Full Multi-Node KRaft Setup**
This repo focuses on Kafka concepts and Spring Boot integration, so it keeps KRaft to this one getting-started manifest. For a proper multi-broker, multi-controller KRaft cluster template (separate controller/broker roles, multiple voters for real quorum tolerance), see the dedicated **[`Kafka-Kraft-template`](https://github.com/Ashfaqbs/Kafka-Kraft-template)** repo.

## **Gotchas**
- `KAFKA_CONTROLLER_QUORUM_VOTERS` must list the *same* node IDs and addresses on every node in the cluster — a mismatch here is the single most common "cluster won't form" issue when moving from a single-node to multi-node KRaft setup.
- Combined `broker,controller` mode (as in the manifest above) is fine for local dev/learning, exactly like this repo's other single-broker examples, but production KRaft clusters typically dedicate separate nodes to each role for isolation and clearer capacity planning.
- There is no live migration path from an old ZooKeeper-mode cluster's data into a KRaft cluster's metadata log — moving an existing cluster to KRaft is a supported but distinct migration procedure, not a config flag flip.
