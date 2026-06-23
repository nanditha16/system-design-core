package com.coresys.ingestion.ebay.publish;

import com.coresys.common.events.ebay.*;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Mirrors InstagramEventPublisher pattern exactly.
 * Key = sellerId/listingId → ordered per entity, enables per-entity compaction.
 */
@Profile("ebay")
@Service
public class EbayEventPublisher {

    private final KafkaTemplate<String, Object> kafka;

    public EbayEventPublisher(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    public Mono<ListingEvent> publishListingEvent(ListingEvent event) {
        return Mono.fromFuture(
                kafka.send(EbayTopics.LISTINGS_INCOMING, event.sellerId(), event)
                     .thenApply(r -> event));
    }

    public Mono<InventoryEvent> publishInventoryEvent(InventoryEvent event) {
        return Mono.fromFuture(
                kafka.send(EbayTopics.INVENTORY_EVENTS, event.listingId(), event)
                     .thenApply(r -> event));
    }

    public Mono<SellerEvent> publishSellerEvent(SellerEvent event) {
        return Mono.fromFuture(
                kafka.send(EbayTopics.SELLER_EVENTS, event.sellerId(), event)
                     .thenApply(r -> event));
    }
}
