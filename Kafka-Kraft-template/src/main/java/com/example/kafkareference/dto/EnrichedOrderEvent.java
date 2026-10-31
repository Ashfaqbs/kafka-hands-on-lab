package com.example.kafkareference.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Outgoing message produced to orders.enriched after transformation.
 *
 * record — same as RawOrderEvent: immutable, no boilerplate, Java 17.
 *   Jackson serializes this to JSON automatically via JsonSerializer
 *   (configured in KafkaProducerConfig). No @JsonProperty needed as long
 *   as field names match what downstream consumers expect.
 *
 * Enrichment fields added by the processing step:
 *   totalAmount  — computed from price × quantity (not in the raw event)
 *   status       — always "PROCESSED" at this stage
 *   processedAt  — timestamp when the Spring Boot consumer processed the message
 *
 * This event is published to Kafka (orders.enriched). It is NOT saved to
 * PostgreSQL separately — only the ProcessedOrder entity goes to the DB.
 * Downstream consumers of orders.enriched receive this and act on it independently.
 */
public record EnrichedOrderEvent(
    UUID    orderId,
    String  customerId,
    String  product,
    int     quantity,
    double  price,
    double  totalAmount,    // price * quantity — the enrichment
    String  currency,
    String  status,         // "PROCESSED"
    Instant processedAt     // when this consumer processed it
) {}
