package com.coresys.state.ebay.consumer;

import com.coresys.common.events.ebay.EbayTopics;
import com.coresys.common.events.ebay.SellerEvent;
import com.coresys.state.ebay.service.SellerStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Profile("ebay")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class SellerStateConsumer {

    private static final Logger log = LoggerFactory.getLogger(SellerStateConsumer.class);
    private final SellerStateService stateService;

    public SellerStateConsumer(SellerStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = EbayTopics.SELLER_EVENTS, groupId = "ebay-seller-state")
    public void onSellerEvent(SellerEvent event, Acknowledgment ack) {
        try {
            stateService.apply(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("SellerState failed sellerId={}: {}", event.sellerId(), e.getMessage(), e);
            throw e;
        }
    }
}
