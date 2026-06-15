package com.coresys.common.events.banking;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Canonical payment event flowing through Kafka.
 * eventId   -> consumer-side dedup key
 * paymentId -> business key, unique in DB
 */
public record PaymentEvent(
        String eventId,
        String paymentId,
        String idempotencyKey,
        String userId,
        BigDecimal amount,
        String currency,
        PaymentType type,
        PaymentRegion region,
        PaymentStatus status,
        String coreBankingRef,   // set after core banking ACK
        Instant occurredAt
) {
    public PaymentEvent withStatus(PaymentStatus next) {
        return new PaymentEvent(eventId, paymentId, idempotencyKey, userId,
                amount, currency, type, region, next, coreBankingRef, Instant.now());
    }

    public PaymentEvent withCoreBankingRef(String ref) {
        return new PaymentEvent(eventId, paymentId, idempotencyKey, userId,
                amount, currency, type, region, status, ref, Instant.now());
    }
}
