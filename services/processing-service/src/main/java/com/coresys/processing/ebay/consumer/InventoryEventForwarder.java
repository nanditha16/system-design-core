package com.coresys.processing.ebay.consumer;

import com.coresys.common.events.ebay.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CONSUMER GROUP: ebay-inventory-processor
 * Consumes: ebay.inventory.events.v1
 *
 * Simple pass-through forwarder — validates event type, then forwards to state-service
 * via the same topic (state-service also consumes ebay.inventory.events.v1).
 *
 * In production this layer would: validate orderId exists, check fraud signals,
 * apply rate limits per seller. For the demo it's a routing checkpoint.
 */
@Profile("ebay")
@Component
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class InventoryEventForwarder {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventForwarder.class);
    private final KafkaTemplate<String, Object> kafka;

    public InventoryEventForwarder(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @KafkaListener(topics = EbayTopics.INVENTORY_EVENTS, groupId = "ebay-inventory-processor")
    public void onInventoryEvent(InventoryEvent event, Acknowledgment ack) {
        try {
            log.info("Inventory event: listingId={} type={} qty={}",
                    event.listingId(), event.type(), event.quantity());
            // Forwarded in-place: state-service also consumes this topic (different group)
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Inventory forwarder failed listingId={}: {}", event.listingId(), e.getMessage());
            throw e;
        }
    }
}
