package dev.lydtech.dispatch.service;

import dev.lydtech.dispatch.client.StockServiceClient;
import dev.lydtech.dispatch.message.DispatchCompleted;
import dev.lydtech.dispatch.message.DispatchPrepared;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

@Service
@Slf4j
@RequiredArgsConstructor
public class DispatchService {
    public static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    public static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    private final KafkaTemplate<String, Object> kafkaProducer;
    private static final UUID APPLICATION_ID = UUID.randomUUID();
    private final StockServiceClient stockServiceClient;


    public void process(String key, OrderCreated payload) throws Exception {
        String available = stockServiceClient.checkAvailability(payload.getItem());
        if (Boolean.parseBoolean(available)) {
            OrderDispatched orderDispatched = OrderDispatched
                    .builder()
                    .orderId(payload.getOrderId())
                    .processedById(APPLICATION_ID)
                    .notes("Dispatched: " + payload.getItem())
                    .build();
            DispatchPrepared dispatchPrepared = DispatchPrepared.builder().orderId(payload.getOrderId()).build();
            DispatchCompleted dispatchCompleted = DispatchCompleted.builder().orderId(payload.getOrderId()).date(LocalDateTime.now().atOffset(ZoneOffset.UTC).format(ISO_OFFSET_DATE_TIME)).build();

            kafkaProducer.send(ORDER_DISPATCHED_TOPIC, key, orderDispatched).get();
            kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, dispatchPrepared).get();
            kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, dispatchCompleted).get();
            log.info("Sent messages: key({}) - orderId: {} - processedById: {}", key, payload.getOrderId(), APPLICATION_ID);
        } else {
            log.info("Item {} is not available", payload.getItem());
        }

    }
}
