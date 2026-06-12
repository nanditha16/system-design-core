package com.coresys.common.events;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Canonical event flowing through Kafka.
 * eventId       -> consumer-side deduplication key (exactly-once approximation)
 * transactionId -> business key, unique constraint in DB
 */
public record TransactionEvent(
        String eventId,
        String transactionId,
        String idempotencyKey,
        BigDecimal amount,
        String currency,
        String region,
        String type,
        TransactionStatus status,
        Instant occurredAt
) {
    public TransactionEvent withStatus(TransactionStatus newStatus) {
        return new TransactionEvent(eventId, transactionId, idempotencyKey,
                amount, currency, region, type, newStatus, Instant.now());
    }
}
