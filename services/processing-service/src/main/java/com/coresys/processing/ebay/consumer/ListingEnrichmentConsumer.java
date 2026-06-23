package com.coresys.processing.ebay.consumer;

import com.coresys.common.events.ebay.*;
import com.coresys.processing.ebay.enrichment.ListingEnrichmentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CONSUMER GROUP: ebay-listing-enrichment
 * Consumes: ebay.listings.incoming.v1 (DRAFT listings)
 *
 * Flow:
 *   1. Receive DRAFT listing event
 *   2. Run 4 enrichment tasks in parallel (CompletableFuture.allOf)
 *   3. If all pass → republish enriched=true to ebay.listings.enriched.v1
 *   4. If any fail → publish to ebay.dlq.v1 → listing stays DRAFT
 *
 * Manual ack: offset committed ONLY after enrichment + downstream publish succeed.
 * Failure → DefaultErrorHandler (exponential backoff) → ebay.dlq.v1
 */
@Profile("ebay")
@Component
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class ListingEnrichmentConsumer {

    private static final Logger log = LoggerFactory.getLogger(ListingEnrichmentConsumer.class);

    private final ListingEnrichmentService enrichment;
    private final KafkaTemplate<String, Object> kafka;

    public ListingEnrichmentConsumer(ListingEnrichmentService enrichment,
                                      KafkaTemplate<String, Object> kafka) {
        this.enrichment = enrichment;
        this.kafka = kafka;
    }

    @KafkaListener(topics = EbayTopics.LISTINGS_INCOMING, groupId = "ebay-listing-enrichment")
    public void onListing(ListingEvent event, Acknowledgment ack) {
        try {
            ListingEnrichmentService.EnrichmentResult result = enrichment.enrich(event).join();
                
            ListingEvent enriched = new ListingEvent(
                java.util.UUID.randomUUID().toString(),
                event.listingId(),
                event.idempotencyKey(),
                event.sellerId(),
                event.productId(),
                event.price(),
                event.currency(),
                event.region(),
                event.initialQty(),
                true,
                result.categoryValid(),
                result.taxRate(),
                result.compliant(),
                result.processedImageUrl(),
                result.allPassed() ? ListingStatus.ACTIVE : ListingStatus.DRAFT,
                event.occurredAt());

            String targetTopic = result.allPassed()
                    ? EbayTopics.LISTINGS_ENRICHED
                    : EbayTopics.DLQ;

            kafka.send(targetTopic, enriched.sellerId(), enriched).join();
            ack.acknowledge();

            log.info("Listing enrichment: listingId={} → {} (allPassed={})",
                    event.listingId(), targetTopic, result.allPassed());

        } catch (Exception e) {
            log.error("Enrichment failed listingId={}: {}", event.listingId(), e.getMessage(), e);
            throw e;
        }
    }
}
