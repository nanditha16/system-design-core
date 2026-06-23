package com.coresys.state.ebay.consumer;

import com.coresys.common.events.ebay.EbayTopics;
import com.coresys.common.events.ebay.ListingEvent;
import com.coresys.state.ebay.service.ListingStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CONSUMER GROUP: ebay-listing-state
 * Consumes: ebay.listings.enriched.v1 (enriched=true, status=ACTIVE listings)
 *
 * Offset committed ONLY after DB write succeeds.
 * Crash between consume and commit → redelivery → processedEvents dedup absorbs it.
 */
@Component
@Profile("ebay")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class ListingStateConsumer {

    private static final Logger log = LoggerFactory.getLogger(ListingStateConsumer.class);
    private final ListingStateService stateService;

    public ListingStateConsumer(ListingStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = {EbayTopics.LISTINGS_INCOMING, EbayTopics.LISTINGS_ENRICHED}, groupId = "ebay-listing-state")
    public void onEnrichedListing(ListingEvent event, Acknowledgment ack) {
        try {
            stateService.apply(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("ListingState failed listingId={}: {}", event.listingId(), e.getMessage(), e);
            throw e;
        }
    }
}
