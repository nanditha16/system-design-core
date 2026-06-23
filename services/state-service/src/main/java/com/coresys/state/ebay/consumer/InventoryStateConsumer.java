package com.coresys.state.ebay.consumer;

import com.coresys.common.events.ebay.EbayTopics;
import com.coresys.common.events.ebay.InventoryEvent;
import com.coresys.state.ebay.service.InventoryStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CONSUMER GROUP: ebay-inventory-state
 * Consumes: ebay.inventory.events.v1
 * Uses optimistic locking via InventoryStateService to prevent overselling.
 */
@Component
@Profile("ebay")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class InventoryStateConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryStateConsumer.class);
    private final InventoryStateService stateService;

    public InventoryStateConsumer(InventoryStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = EbayTopics.INVENTORY_EVENTS, groupId = "ebay-inventory-state")
    public void onInventoryEvent(InventoryEvent event, Acknowledgment ack) {
        try {
            stateService.apply(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("InventoryState failed listingId={}: {}", event.listingId(), e.getMessage(), e);
            throw e;
        }
    }
}
