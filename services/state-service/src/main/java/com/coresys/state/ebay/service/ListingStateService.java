package com.coresys.state.ebay.service;

import com.coresys.common.events.ebay.*;
import com.coresys.state.domain.ProcessedEventEntity;
import com.coresys.state.domain.ProcessedEventRepository;
import com.coresys.state.ebay.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ListingStateService {

    private static final Logger log = LoggerFactory.getLogger(ListingStateService.class);

    private final ListingRepository listings;
    private final InventoryRepository inventory;
    private final ProcessedEventRepository processedEvents;

    public ListingStateService(ListingRepository listings, InventoryRepository inventory,
                               ProcessedEventRepository processedEvents) {
        this.listings = listings; this.inventory = inventory;
        this.processedEvents = processedEvents;
    }

    @Transactional
    public void apply(ListingEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate listing event {} skipped", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEventEntity(event.eventId()));

        if (!event.enriched()) {
            try {
                listings.save(new ListingEntity(
                        event.listingId(), event.sellerId(), event.productId(),
                        event.price(), event.currency(), event.region(),
                        event.idempotencyKey()));
                log.info("Listing DRAFT saved: {}", event.listingId());
            } catch (DataIntegrityViolationException e) {
                log.warn("Idempotent skip listing={}", event.listingId());
            }
        } else {
            listings.findById(event.listingId()).ifPresentOrElse(listing -> {
                listing.activate(event.taxRate(), event.processedImageUrl());
                listings.save(listing);
                if (!inventory.existsById(event.listingId())) {
                    inventory.save(new InventoryEntity(event.listingId(), event.initialQty()));
                    log.info("Inventory created: listingId={} qty={}", event.listingId(), event.initialQty());
                }
                log.info("Listing ACTIVE: {} taxRate={}%", event.listingId(),
                        String.format("%.0f", event.taxRate() * 100));
            }, () -> log.warn("Listing not found for activation: {}", event.listingId()));
        }
    }
}
