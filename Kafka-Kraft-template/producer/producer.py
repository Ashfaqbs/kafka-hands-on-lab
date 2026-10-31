"""
Kafka producer — sends RawOrderEvent objects to orders.raw topic.

Auth: SASL/SCRAM-SHA-512 (same credentials created by kafka-setup in docker-compose)

Run:
    pip install -r requirements.txt
    python producer.py                  # sends 10 orders
    python producer.py --count 50       # sends 50 orders
    python producer.py --interval 0.5   # sends every 500ms

Message schema (JSON):
    {
        "orderId":    "<uuid>",
        "customerId": "customer-<N>",
        "product":    "<product name>",
        "quantity":   <int>,
        "price":      <float>,
        "currency":   "USD",
        "timestamp":  "<ISO-8601>"
    }
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "app-producer",
    "sasl.password": "producer-secret",

    # Producer reliability settings
    "acks": "all",                  # wait for all in-sync replicas to ack
    "enable.idempotence": True,     # exactly-once producer semantics (dedup on broker side)
    "retries": 5,                   # retry transient send failures
    "retry.backoff.ms": 500,
    "max.in.flight.requests.per.connection": 5,  # safe with idempotence enabled
    "compression.type": "lz4",      # compress batches before sending
    "linger.ms": 5,                 # wait 5ms to batch messages before sending
}

TOPIC = "orders.raw"

PRODUCTS = [
    ("MacBook Pro 14", 1999.99),
    ("Sony WH-1000XM5", 349.99),
    ("iPad Air", 599.99),
    ("Samsung Galaxy S24", 799.99),
    ("LG OLED TV 55\"", 1299.99),
    ("Nintendo Switch OLED", 349.99),
    ("Mechanical Keyboard", 129.99),
    ("USB-C Hub 7-in-1", 49.99),
]

# ─── Delivery callback ────────────────────────────────────────────────────────

def on_delivery(err, msg):
    """Called by the producer after each message is acknowledged by the broker."""
    if err:
        log.error("Delivery failed: topic=%s partition=%s offset=%s error=%s",
                  msg.topic(), msg.partition(), msg.offset(), err)
    else:
        log.info("Delivered: topic=%s partition=%d offset=%d key=%s",
                 msg.topic(), msg.partition(), msg.offset(), msg.key().decode())

# ─── Message factory ──────────────────────────────────────────────────────────

def make_order() -> dict:
    product, price = random.choice(PRODUCTS)
    return {
        "orderId":    str(uuid.uuid4()),
        "customerId": f"customer-{random.randint(1, 20)}",
        "product":    product,
        "quantity":   random.randint(1, 5),
        "price":      price,
        "currency":   "USD",
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }

# ─── Main ─────────────────────────────────────────────────────────────────────

def main(count: int, interval: float):
    producer = Producer(KAFKA_CONFIG)
    log.info("Connected to Kafka. Sending %d orders to topic '%s'", count, TOPIC)

    sent = 0
    try:
        for i in range(count):
            order = make_order()

            producer.produce(
                topic=TOPIC,
                key=order["orderId"],           # partition by orderId for ordering guarantee
                value=json.dumps(order),
                headers={
                    "source": b"python-producer",
                    "version": b"1.0",
                },
                on_delivery=on_delivery,
            )

            # Poll to trigger delivery callbacks and prevent buffer from filling
            producer.poll(0)

            sent += 1
            log.info("[%d/%d] Queued orderId=%s product='%s' qty=%d",
                     i + 1, count, order["orderId"], order["product"], order["quantity"])

            if interval > 0 and i < count - 1:
                time.sleep(interval)

    except KafkaException as e:
        log.error("Kafka error: %s", e)
        raise
    finally:
        # Block until all queued messages are delivered (with 30s timeout)
        remaining = producer.flush(timeout=30)
        if remaining > 0:
            log.warning("%d message(s) were NOT delivered before timeout", remaining)
        else:
            log.info("All %d messages delivered successfully", sent)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka order producer")
    parser.add_argument("--count", type=int, default=10, help="Number of orders to send")
    parser.add_argument("--interval", type=float, default=0.2, help="Seconds between messages")
    args = parser.parse_args()

    main(count=args.count, interval=args.interval)
