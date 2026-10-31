-- ─── processed_orders ────────────────────────────────────────────────────────
--
-- Stores every successfully processed order after enrichment.
--
-- Idempotency: order_id has a UNIQUE constraint. If the same Kafka message is
-- delivered twice (at-least-once delivery guarantee), the second attempt hits a
-- unique violation which the service catches and skips — safe re-processing.
--
-- Kafka metadata (topic, partition, offset) is stored for debugging and auditing:
-- you can always trace a DB row back to the exact Kafka offset that produced it.

CREATE TABLE processed_orders
(
    id              BIGSERIAL       NOT NULL,
    order_id        UUID            NOT NULL,     -- business key from the Kafka message
    customer_id     VARCHAR(100)    NOT NULL,
    product         VARCHAR(255)    NOT NULL,
    quantity        INT             NOT NULL,
    price           DECIMAL(10, 2)  NOT NULL,
    total_amount    DECIMAL(10, 2)  NOT NULL,     -- computed: price * quantity
    currency        VARCHAR(3)      NOT NULL DEFAULT 'USD',
    status          VARCHAR(50)     NOT NULL DEFAULT 'PROCESSED',
    kafka_topic     VARCHAR(255)    NOT NULL,
    kafka_partition INT             NOT NULL,
    kafka_offset    BIGINT          NOT NULL,
    processed_at    TIMESTAMPTZ     NOT NULL,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_processed_orders PRIMARY KEY (id)
);

-- Idempotency index — prevents duplicate processing of the same order
CREATE UNIQUE INDEX idx_processed_orders_order_id ON processed_orders (order_id);

-- Query pattern: look up orders by customer
CREATE INDEX idx_processed_orders_customer_id ON processed_orders (customer_id);

-- Query pattern: find orders by kafka offset for debugging
CREATE INDEX idx_processed_orders_kafka ON processed_orders (kafka_topic, kafka_partition, kafka_offset);
