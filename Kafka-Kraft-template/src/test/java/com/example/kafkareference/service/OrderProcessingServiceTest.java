package com.example.kafkareference.service;

import com.example.kafkareference.dto.RawOrderEvent;
import com.example.kafkareference.exception.NonRetryableProcessingException;
import com.example.kafkareference.producer.EnrichedOrderProducer;
import com.example.kafkareference.repository.ProcessedOrderRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderProcessingServiceTest {

    @Mock private ProcessedOrderRepository repository;
    @Mock private EnrichedOrderProducer producer;

    private OrderProcessingService service;

    @BeforeEach
    void setUp() {
        service = new OrderProcessingService(repository, producer);
    }

    @Test
    void shouldProcessValidOrderAndSendDownstream() {
        RawOrderEvent event = validEvent();
        when(repository.existsByOrderId(event.orderId())).thenReturn(false);

        service.process(event, "orders.raw", 0, 100L);

        verify(repository).save(any());
        verify(producer).send(any());
    }

    @Test
    void shouldSkipDuplicateMessage() {
        RawOrderEvent event = validEvent();
        when(repository.existsByOrderId(event.orderId())).thenReturn(true);

        service.process(event, "orders.raw", 0, 100L);

        verify(repository, never()).save(any());
        verify(producer, never()).send(any());
    }

    @Test
    void shouldComputeTotalAmountCorrectly() {
        RawOrderEvent event = new RawOrderEvent(
            UUID.randomUUID(), "c1", "MacBook", 3, 1999.99, "USD", Instant.now());
        when(repository.existsByOrderId(event.orderId())).thenReturn(false);

        var captor = ArgumentCaptor.forClass(com.example.kafkareference.dto.EnrichedOrderEvent.class);

        service.process(event, "orders.raw", 0, 1L);

        verify(producer).send(captor.capture());
        assertThat(captor.getValue().totalAmount()).isEqualTo(5999.97);
    }

    @Test
    void shouldThrowNonRetryableForNegativePrice() {
        RawOrderEvent event = new RawOrderEvent(
            UUID.randomUUID(), "c1", "MacBook", 1, -50.0, "USD", Instant.now());
        when(repository.existsByOrderId(any())).thenReturn(false);

        assertThatThrownBy(() -> service.process(event, "orders.raw", 0, 1L))
            .isInstanceOf(NonRetryableProcessingException.class)
            .hasMessageContaining("price must be > 0");
    }

    @Test
    void shouldThrowNonRetryableForZeroQuantity() {
        RawOrderEvent event = new RawOrderEvent(
            UUID.randomUUID(), "c1", "MacBook", 0, 999.0, "USD", Instant.now());
        when(repository.existsByOrderId(any())).thenReturn(false);

        assertThatThrownBy(() -> service.process(event, "orders.raw", 0, 1L))
            .isInstanceOf(NonRetryableProcessingException.class)
            .hasMessageContaining("quantity must be > 0");
    }

    @Test
    void shouldThrowNonRetryableForBlankCustomerId() {
        RawOrderEvent event = new RawOrderEvent(
            UUID.randomUUID(), "", "MacBook", 1, 999.0, "USD", Instant.now());
        when(repository.existsByOrderId(any())).thenReturn(false);

        assertThatThrownBy(() -> service.process(event, "orders.raw", 0, 1L))
            .isInstanceOf(NonRetryableProcessingException.class);
    }

    private RawOrderEvent validEvent() {
        return new RawOrderEvent(
            UUID.randomUUID(), "customer-1", "MacBook Pro 14",
            2, 1999.99, "USD", Instant.now()
        );
    }
}
