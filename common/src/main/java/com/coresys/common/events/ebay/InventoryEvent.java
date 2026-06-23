package com.coresys.common.events.ebay;

import java.time.Instant;

/**
 * Inventory event — append-only audit log entry.
 * Consumer: InventoryStateConsumer → updates InventoryEntity via optimistic locking.
 */
public record InventoryEvent(
        String eventId,
        String listingId,
        InventoryEventType type,
        int quantity,
        String orderId,
        Instant occurredAt
) {}
