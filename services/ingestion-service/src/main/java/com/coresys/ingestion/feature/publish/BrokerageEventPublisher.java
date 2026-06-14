package com.coresys.ingestion.feature.publish;

import com.coresys.common.events.brokerage.BrokerageTopics;
import com.coresys.common.events.brokerage.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Dedicated Kafka publisher for brokerage orders.
 * Sits alongside KafkaEventPublisher (generic) — same pattern, different topic.
 * Key = orderId -> per-order partition ordering.
 */
@Service
public class BrokerageEventPublisher {

    private final KafkaTemplate<String, Object> kafka;

    public BrokerageEventPublisher(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    public Mono<OrderEvent> publish(OrderEvent event) {
        return Mono.fromFuture(
                kafka.send(BrokerageTopics.ORDERS_INCOMING, event.orderId(), event)
                     .thenApply(r -> event));
    }
}
