package dev.lydtech.dispatch.service;

import dev.lydtech.dispatch.message.DispatchPrepared;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class DispatchService {
    public static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    public static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    private final KafkaTemplate<String, Object> kafkaProducer;
    private static final UUID APPLICATION_ID = UUID.randomUUID();


    public void process(String key, OrderCreated payload) throws Exception {
        OrderDispatched orderDispatched = OrderDispatched
                .builder()
                .orderId(payload.getOrderId())
                .processedById(APPLICATION_ID)
                .notes("Dispatched: "+ payload.getItem())
                .build();
        DispatchPrepared dispatchPrepared = DispatchPrepared.builder().orderId(payload.getOrderId()).build();

        kafkaProducer.send(ORDER_DISPATCHED_TOPIC,key, orderDispatched).get();
        kafkaProducer.send(DISPATCH_TRACKING_TOPIC,key, dispatchPrepared).get();
        log.info("Sent messages: key({}) - orderId: {} - processedById: {}",key, payload.getOrderId(), APPLICATION_ID);
    }
}
