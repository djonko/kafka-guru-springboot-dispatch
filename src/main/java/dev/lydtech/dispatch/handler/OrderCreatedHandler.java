package dev.lydtech.dispatch.handler;

import dev.lydtech.dispatch.exception.NotRetryableException;
import dev.lydtech.dispatch.exception.RetryableException;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static dev.lydtech.dispatch.DispatchConfiguration.ORDER_CREATED_TOPIC;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCreatedHandler {

    private final DispatchService dispatchService;

    @KafkaListener(
            id = "orderConsumerClient",
            topics = ORDER_CREATED_TOPIC,
            groupId = "dispatch.order.created.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload) {
        log.info("Received message: partition({}) key({}) payload {}", partition, key, payload);
        try {
            dispatchService.process(key, payload);
        } catch (RetryableException e) {
            log.warn("Retryable exception: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("NotRetryable exception: {}", e.getMessage());
            throw new NotRetryableException(e);
        }
    }
}
