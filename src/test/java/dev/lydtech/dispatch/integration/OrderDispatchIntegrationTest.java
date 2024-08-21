package dev.lydtech.dispatch.integration;

import dev.lydtech.dispatch.DispatchConfiguration;
import dev.lydtech.dispatch.message.DispatchCompleted;
import dev.lydtech.dispatch.message.DispatchPrepared;
import dev.lydtech.dispatch.message.OrderCreated;
import dev.lydtech.dispatch.message.OrderDispatched;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static dev.lydtech.dispatch.integration.WiremockUtils.stubWiremock;
import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@SpringBootTest(classes = {DispatchConfiguration.class})
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true)
@AutoConfigureWireMock(port = 0)
public class OrderDispatchIntegrationTest {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private KafkaTestListener kafkaTestListener;

    @BeforeEach
    public void setUp(@Autowired EmbeddedKafkaBroker broker, @Autowired KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        kafkaTestListener.dispatchPreparingCounter.set(0);
        kafkaTestListener.orderDispatchedCounter.set(0);
        kafkaTestListener.dispatchCompletingCounter.set(0);
        kafkaTestListener.orderCreatedDLTCounter.set(0);
        WiremockUtils.reset();
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(container -> ContainerTestUtils.waitForAssignment(container, Objects.requireNonNull(container.getContainerProperties().getTopics()).length * broker.getPartitionsPerTopic()));
    }

    @Test
    public void testOrderDispatchFlow_Success() throws Exception {
        stubWiremock("/api/stock?item=my-item", 200, "true");
        OrderCreated orderCreated = OrderCreated.builder().orderId(UUID.randomUUID()).item("my-item").build();
        String key = randomUUID().toString();
        sendMessage(DispatchConfiguration.ORDER_CREATED_TOPIC, key, orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.dispatchPreparingCounter::get, equalTo(1));
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.dispatchCompletingCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.orderDispatchedCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.orderCreatedDLTCounter::get, equalTo(0));
    }

    @Test
    public void testOrderDispatchFlow_NotRetryableException() throws Exception {
        stubWiremock("/api/stock?item=my-item", 400, "Bad Request");
        OrderCreated orderCreated = OrderCreated.builder().orderId(UUID.randomUUID()).item("my-item").build();
        String key = randomUUID().toString();
        sendMessage(DispatchConfiguration.ORDER_CREATED_TOPIC, key, orderCreated);
        TimeUnit.SECONDS.sleep(3);
        assertThat(kafkaTestListener.orderCreatedDLTCounter.get(), equalTo(1));
        assertThat(kafkaTestListener.orderDispatchedCounter.get(), equalTo(0));
        assertThat(kafkaTestListener.dispatchCompletingCounter.get(), equalTo(0));
        assertThat(kafkaTestListener.dispatchPreparingCounter.get(), equalTo(0));
    }

    @Test
    public void testOrderDispatchFlow_RetryThenSuccess() throws Exception {
        stubWiremock("/api/stock?item=my-item", 503, "Service Unavailable", "failOnce", STARTED, "successNextTime");
        stubWiremock("/api/stock?item=my-item", 200, "true", "failOnce", "successNextTime", "successNextTime");
        OrderCreated orderCreated = OrderCreated.builder().orderId(UUID.randomUUID()).item("my-item").build();
        String key = randomUUID().toString();
        sendMessage(DispatchConfiguration.ORDER_CREATED_TOPIC, key, orderCreated);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.dispatchPreparingCounter::get, equalTo(1));
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.dispatchCompletingCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.orderDispatchedCounter::get, equalTo(1));
        await().atMost(1, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.orderDispatchedCounter::get, equalTo(0));

    }

    @Test
    public void testOrderDispatchFlow_RetryUntilFailure() throws Exception {
        stubWiremock("/api/stock?item=my-item", 503, "Service unavailable");
        OrderCreated orderCreated = OrderCreated.builder().orderId(UUID.randomUUID()).item("my-item").build();
        String key = randomUUID().toString();
        sendMessage(DispatchConfiguration.ORDER_CREATED_TOPIC, key, orderCreated);
        await().atMost(5, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).until(kafkaTestListener.orderCreatedDLTCounter::get, equalTo(1));
        assertThat(kafkaTestListener.orderDispatchedCounter.get(), equalTo(0));
        assertThat(kafkaTestListener.dispatchCompletingCounter.get(), equalTo(0));
        assertThat(kafkaTestListener.dispatchPreparingCounter.get(), equalTo(0));
    }

    private void sendMessage(String topic, String key, Object data) throws Exception {
        kafkaTemplate.send(
                MessageBuilder
                        .withPayload(data).setHeader(KafkaHeaders.TOPIC, topic)
                        .setHeader(KafkaHeaders.KEY, key)
                        .build()
        ).get();
    }

    @Configuration
    static class TestConfig {
        @Bean
        public KafkaTestListener testListener() {
            return new KafkaTestListener();
        }
    }

    @KafkaListener(groupId = "KafkaIntegrationTest", topics = {DispatchConfiguration.DISPATCH_TRACKING_TOPIC, DispatchConfiguration.ORDER_DISPATCHED_TOPIC, DispatchConfiguration.ORDER_CREATED_DLT_TOPIC})
    public static class KafkaTestListener {

        AtomicInteger dispatchPreparingCounter = new AtomicInteger(0);
        AtomicInteger dispatchCompletingCounter = new AtomicInteger(0);
        AtomicInteger orderDispatchedCounter = new AtomicInteger(0);
        AtomicInteger orderCreatedDLTCounter = new AtomicInteger(0);

        @KafkaHandler
        void receivedDispatchPreparing(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload DispatchPrepared payload) {
            log.debug("Received dispatch preparing:  key({}) {}", key, payload);
            Assertions.assertNotNull(key);
            Assertions.assertNotNull(payload);
            dispatchPreparingCounter.incrementAndGet();
        }

        @KafkaHandler
        void receivedDispatchCompleting(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload DispatchCompleted payload) {
            log.debug("Received dispatch completing:  key({}) {}", key, payload);
            Assertions.assertNotNull(key);
            Assertions.assertNotNull(payload);
            dispatchCompletingCounter.incrementAndGet();
        }


        @KafkaHandler
        void receivedOrderDispatched(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderDispatched payload) {
            log.debug("Received order dispatched: key({}) {}", key, payload);
            Assertions.assertNotNull(key);
            Assertions.assertNotNull(payload);
            orderDispatchedCounter.incrementAndGet();
        }

        @KafkaHandler
        void receivedOrderCreatedDLT(@Header(KafkaHeaders.RECEIVED_KEY) String key, @Payload OrderCreated payload) {
            log.debug("Received order created DLT: key({}) {}", key, payload);
            Assertions.assertNotNull(key);
            Assertions.assertNotNull(payload);
            orderCreatedDLTCounter.incrementAndGet();
        }
    }
}
