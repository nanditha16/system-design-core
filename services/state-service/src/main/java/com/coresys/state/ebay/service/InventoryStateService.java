package com.coresys.state.ebay.service;

import com.coresys.common.events.ebay.*;
import com.coresys.state.domain.ProcessedEventEntity;
import com.coresys.state.domain.ProcessedEventRepository;
import com.coresys.state.ebay.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * OPTIMISTIC LOCKING for inventory — prevents overselling.
 *
 * JPA @Version generates:
 *   UPDATE ebay_inventory SET available_qty=?, version=version+1
 *   WHERE listing_id=? AND version=?
 *
 * rows_affected=0 → ObjectOptimisticLockingFailureException
 * → retry up to MAX_RETRIES (3)
 * → if still failing → throw → Kafka DefaultErrorHandler retries → DLQ
 *
 * WHY optimistic over pessimistic (SELECT FOR UPDATE):
 *   Reads vastly outnumber writes (10,000 QPS reads vs ~6 writes/sec per listing).
 *   Pessimistic holds a DB row lock for entire transaction duration → kills read throughput.
 *   Optimistic only checks at commit time → no lock held during business logic.
 */
@Service
public class InventoryStateService {

    private static final Logger log = LoggerFactory.getLogger(InventoryStateService.class);
    private static final int MAX_RETRIES = 3;

    private final InventoryRepository inventory;
    private final ListingRepository listings;
    private final ProcessedEventRepository processedEvents;

    public InventoryStateService(InventoryRepository inventory, ListingRepository listings,
                                  ProcessedEventRepository processedEvents) {
        this.inventory = inventory; this.listings = listings;
        this.processedEvents = processedEvents;
    }

    @Transactional
    public void apply(InventoryEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate inventory event {} skipped", event.eventId());
            return;
        }

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                doApply(event);
                processedEvents.save(new ProcessedEventEntity(event.eventId()));
                return;
            } catch (ObjectOptimisticLockingFailureException e) {
                log.warn("Optimistic lock conflict listingId={} attempt={}/{}", 
                        event.listingId(), attempt, MAX_RETRIES);
                if (attempt == MAX_RETRIES) throw e;
            }
        }
    }

    private void doApply(InventoryEvent event) {
        InventoryEntity inv = inventory.findById(event.listingId())
                .orElseThrow(() -> new RuntimeException("Inventory not found: " + event.listingId()));

        switch (event.type()) {
            case ADD     -> inv.add(event.quantity());
            case REMOVE  -> { inv.add(-event.quantity()); }
            case RESERVE -> inv.reserve(event.quantity());
            case RELEASE -> inv.release(event.quantity());
            case SELL    -> {
                inv.sell(event.quantity());
                if (inv.getAvailableQty() == 0) {
                    listings.findById(event.listingId())
                            .ifPresent(l -> { l.markOutOfStock(); listings.save(l); });
                }
            }
        }
        inventory.save(inv);
        log.info("Inventory applied: listingId={} type={} qty={} available={}",
                event.listingId(), event.type(), event.quantity(), inv.getAvailableQty());
    }
}
