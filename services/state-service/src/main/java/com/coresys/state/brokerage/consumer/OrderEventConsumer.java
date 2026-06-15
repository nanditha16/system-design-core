package com.coresys.state.brokerage.consumer;

import com.coresys.common.events.brokerage.BrokerageTopics;
import com.coresys.common.events.brokerage.OrderEvent;
import com.coresys.state.brokerage.service.OrderStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * CRITICAL ORDERING: DB commit FIRST, offset commit SECOND.
 * Crash between the two -> redelivery -> processedEvents dedup absorbs it.
 */
@Component
@Profile("brokerage")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private final OrderStateService stateService;

    public OrderEventConsumer(OrderStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = BrokerageTopics.ORDERS_PROCESSED, groupId = "brokerage-state")
    public void onOrder(OrderEvent event, Acknowledgment ack) {
        try {
            stateService.apply(event);
            ack.acknowledge();  // offset commit AFTER DB write
        } catch (Exception e) {
            log.error("State apply failed order={}: {}", event.orderId(), e.getMessage(), e);
            throw e;
        }
    }
}
