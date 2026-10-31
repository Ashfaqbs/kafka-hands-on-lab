package com.example.kafkareference.repository;

import com.example.kafkareference.domain.ProcessedOrder;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrder, Long> {

    /**
     * Idempotency check — called before processing each message.
     * If true, the message was already processed and will be skipped.
     */
    boolean existsByOrderId(UUID orderId);
}
