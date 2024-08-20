package dev.lydtech.dispatch.service;

import dev.lydtech.dispatch.client.StockServiceClient;
import dev.lydtech.dispatch.message.DispatchCompleted;
import dev.lydtech.dispatch.message.DispatchPrepared;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import dev.lydtech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DispatchServiceTest {

    private final String key = randomUUID().toString();
    private DispatchService dispatchService;
    private KafkaTemplate kafkaProducerMock;
    private CompletableFuture completableFuture;
    private StockServiceClient stockServiceClientMock;

    @BeforeEach
    void setUp() {
        kafkaProducerMock = mock(KafkaTemplate.class);
        stockServiceClientMock = mock(StockServiceClient.class);
        dispatchService = new DispatchService(kafkaProducerMock, stockServiceClientMock);
        completableFuture = CompletableFuture.completedFuture(mock(SendResult.class));
    }

    @Test
    void process_Success() throws Exception {
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
        when(kafkaProducerMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(completableFuture);
        when(kafkaProducerMock.send(anyString(), anyString(), any(DispatchPrepared.class))).thenReturn(completableFuture);
        when(kafkaProducerMock.send(anyString(), anyString(), any(DispatchCompleted.class))).thenReturn(completableFuture);
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        dispatchService.process(key, testEvent);
        verify(kafkaProducerMock, times(1)).send(eq(DispatchService.ORDER_DISPATCHED_TOPIC), eq(key), any(OrderDispatched.class));
        verify(kafkaProducerMock, times(1)).send(eq(DispatchService.DISPATCH_TRACKING_TOPIC), eq(key), any(DispatchPrepared.class));
        verify(kafkaProducerMock, times(1)).send(eq(DispatchService.DISPATCH_TRACKING_TOPIC), eq(key), any(DispatchCompleted.class));
        verify(stockServiceClientMock, times(1)).checkAvailability(anyString());
    }

    @Test
    void process_ProducerThrowException() {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Producer failed")).when(kafkaProducerMock)
                .send(eq("order.dispatched"), anyString(), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, testEvent));

        verify(kafkaProducerMock, times(1))
                .send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

        assertEquals(exception.getMessage(), "Producer failed");
    }
}