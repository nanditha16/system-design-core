package com.coresys.common.events.ebay;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Canonical listing event flowing through Kafka.
 *
 * eventId        → consumer-side dedup key (idempotency)
 * listingId      → business key, unique in DB
 * idempotencyKey → X-Idempotency-Key from mutation header — prevents double-create
 * enriched       → false when first published (DRAFT), true after enrichment pipeline
 *
 * enrichment tasks run in parallel via CompletableFuture.allOf():
 *   categoryValid, taxRate, compliant, processedImageUrl
 * All must pass before DRAFT → ACTIVE transition.
 */
public record ListingEvent(
        String eventId,
        String listingId,
        String idempotencyKey,
        String sellerId,
        String productId,
        BigDecimal price,
        String currency,
        String region,
        int initialQty,
        boolean enriched,
        boolean categoryValid,
        double taxRate,
        boolean compliant,
        String processedImageUrl,
        ListingStatus status,
        Instant occurredAt
) {}
