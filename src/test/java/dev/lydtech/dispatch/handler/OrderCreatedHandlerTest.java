package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.exception.NotRetryableException;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.service.DispatchService;
import dev.lydtech.dispatch.util.TestEventData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static java.util.UUID.randomUUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class OrderCreatedHandlerTest {

    private OrderCreatedHandler handler;
    private DispatchService dispatchServiceMock;

    @BeforeEach
    void setUp() {
        dispatchServiceMock = mock(DispatchService.class);
        handler = new OrderCreatedHandler(dispatchServiceMock);
    }

    @Test
    void listen_Success() throws Exception {
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        String key = randomUUID().toString();
        handler.listen(0, key,testEvent);
        verify(dispatchServiceMock, times(1)).process(key,testEvent);
    }

    public OrderCreatedHandlerTest() {
        super();
    }

    @Test
    void listen_ServiceThrowsException() throws Exception {
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Service failed")).when(dispatchServiceMock).process(key, testEvent);
        Exception exception = assertThrows(NotRetryableException.class, () -> handler.listen(0, key, testEvent));
        assertThat(exception.getMessage(), equalTo("java.lang.RuntimeException: Service failed"));
        verify(dispatchServiceMock, times(1)).process(key, testEvent);
    }

}