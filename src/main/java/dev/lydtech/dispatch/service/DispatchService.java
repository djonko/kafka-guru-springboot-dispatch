package dev.lydtech.dispatch.service;

import dev.lydtech.dispatch.message.DispatchPrepared;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DispatchService {
    public static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    public static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    private final KafkaTemplate<String, Object> kafkaProducer;

    public void process(OrderCreated payload) throws Exception {
        OrderDispatched orderDispatched = OrderDispatched
                .builder()
                .orderId(payload.getOrderId()).build();
        DispatchPrepared dispatchPrepared = DispatchPrepared.builder().orderId(payload.getOrderId()).build();

        kafkaProducer.send(ORDER_DISPATCHED_TOPIC, orderDispatched).get();
        kafkaProducer.send(DISPATCH_TRACKING_TOPIC, dispatchPrepared).get();
    }


}
