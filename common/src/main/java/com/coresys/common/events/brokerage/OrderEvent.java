package com.coresys.common.events.brokerage;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderEvent(
        String eventId,
        String orderId,
        String userId,
        String idempotencyKey,
        OrderType type,
        String symbol,
        BigDecimal quantity,
        BigDecimal price,
        OrderStatus status,
        Instant occurredAt
) {
    public OrderEvent withStatus(OrderStatus next) {
        return new OrderEvent(eventId, orderId, userId, idempotencyKey,
                type, symbol, quantity, price, next, Instant.now());
    }
    public BigDecimal totalAmount() { return quantity.multiply(price); }
}
