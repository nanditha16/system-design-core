package com.coresys.processing.brokerage.consumer;

import com.coresys.common.events.brokerage.BrokerageTopics;
import com.coresys.common.events.brokerage.OrderEvent;
import com.coresys.processing.brokerage.routing.OrderRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Consumes orders.incoming.v1 -> validates -> publishes to orders.processed.v1.
 * Manual ack: offset committed ONLY after downstream publish succeeds.
 * Failures -> DefaultErrorHandler (exponential backoff) -> orders.dlq.v1.
 */
@Component
@Profile("brokerage")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderRouter router;
    private final KafkaTemplate<String, Object> kafka;

    public OrderConsumer(OrderRouter router, KafkaTemplate<String, Object> kafka) {
        this.router = router;
        this.kafka = kafka;
    }

    @KafkaListener(topics = BrokerageTopics.ORDERS_INCOMING, groupId = "brokerage-processing")
    public void onOrder(OrderEvent event, Acknowledgment ack) {
        try {
            OrderEvent processed = router.process(event);
            kafka.send(BrokerageTopics.ORDERS_PROCESSED, processed.orderId(), processed).join();
            ack.acknowledge();
            log.info("Order routed: {} {} -> {}", processed.type(), processed.orderId(), processed.status());
        } catch (Exception e) {
            log.error("Order processing failed {}: {}", event.orderId(), e.getMessage(), e);
            throw e;
        }
    }
}
