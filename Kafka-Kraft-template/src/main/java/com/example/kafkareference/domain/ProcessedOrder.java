package com.example.kafkareference.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

// @Entity — tells JPA/Hibernate that this class maps to a database table.
// Hibernate will manage INSERT/UPDATE/SELECT/DELETE for instances of this class.
// Without @Entity, JPA ignores the class entirely.
@Entity

// @Table(name = "processed_orders") — maps this entity to the processed_orders table.
// Without this, Hibernate would derive the table name from the class name (ProcessedOrder → processed_order).
// Explicit naming avoids surprises across different databases and case-sensitivity rules.
@Table(name = "processed_orders")
public class ProcessedOrder {

    // @Id — marks this field as the primary key. Required on every @Entity.
    // JPA uses the @Id field to track entity identity and decide INSERT vs UPDATE.
    @Id

    // @GeneratedValue(strategy = IDENTITY) — delegates PK generation to the database.
    // PostgreSQL's BIGSERIAL column auto-increments on each INSERT.
    // IDENTITY strategy = database-generated. Never set this field manually.
    //
    // Why BIGSERIAL/Long for PK instead of UUID?
    //   Internal PK: fast integer comparison for JOINs and indexing.
    //   order_id (UUID) is the public-facing deduplication key — never exposed as the PK.
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // @Column — maps the field to a specific column, with constraints enforced by both
    // Hibernate (schema generation/validation) and the DB (via the Flyway migration).
    //
    // unique = true: generates a UNIQUE constraint on this column.
    //   This is the hard guard for idempotent processing — even if two threads race
    //   and both pass the existsByOrderId() check simultaneously, only one INSERT
    //   will succeed; the other gets a constraint violation.
    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;

    @Column(name = "customer_id", nullable = false, length = 100)
    private String customerId;

    @Column(name = "product", nullable = false, length = 255)
    private String product;

    @Column(name = "quantity", nullable = false)
    private int quantity;

    // precision = 10, scale = 2: up to 8 digits before decimal, 2 after.
    // Using BigDecimal (not double) to avoid floating-point rounding errors on monetary values.
    @Column(name = "price", nullable = false, precision = 10, scale = 2)
    private BigDecimal price;

    @Column(name = "total_amount", nullable = false, precision = 10, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "status", nullable = false, length = 50)
    private String status;

    // Kafka audit columns — stored with every row so you can:
    //   - Trace any DB row back to its exact Kafka message (topic + partition + offset)
    //   - Replay a specific message if needed (produce it back to the topic)
    //   - Debug consumer lag and ordering issues
    @Column(name = "kafka_topic", nullable = false, length = 255)
    private String kafkaTopic;

    @Column(name = "kafka_partition", nullable = false)
    private int kafkaPartition;

    @Column(name = "kafka_offset", nullable = false)
    private long kafkaOffset;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    // updatable = false — Hibernate will never include this column in an UPDATE statement.
    // The DB sets it on INSERT via DEFAULT NOW(). Java just reads it back.
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    // @PrePersist — JPA lifecycle callback. Hibernate calls this method automatically
    // just before executing the INSERT statement. No explicit call needed.
    // Use this for fields that should be set exactly once at creation time.
    // Alternatives: @CreatedDate from Spring Data Auditing (requires @EnableJpaAuditing).
    @PrePersist
    protected void onCreate() {
        this.createdAt = Instant.now();
    }

    // ─── Getters / Setters ──────────────────────────────────────────────────

    public Long getId() { return id; }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public BigDecimal getPrice() { return price; }
    public void setPrice(BigDecimal price) { this.price = price; }

    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getKafkaTopic() { return kafkaTopic; }
    public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }

    public int getKafkaPartition() { return kafkaPartition; }
    public void setKafkaPartition(int kafkaPartition) { this.kafkaPartition = kafkaPartition; }

    public long getKafkaOffset() { return kafkaOffset; }
    public void setKafkaOffset(long kafkaOffset) { this.kafkaOffset = kafkaOffset; }

    public Instant getProcessedAt() { return processedAt; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }

    public Instant getCreatedAt() { return createdAt; }
}
