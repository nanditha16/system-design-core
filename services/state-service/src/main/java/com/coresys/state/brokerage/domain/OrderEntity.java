package com.coresys.state.brokerage.domain;

import com.coresys.common.events.brokerage.OrderStatus;
import com.coresys.common.events.brokerage.OrderType;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * DB-level exactly-once backstop:
 *  UNIQUE(order_id)       -> duplicate Kafka delivery can never create a second row
 *  UNIQUE(idempotency_key)-> duplicate API call can never create a second order
 */
@Entity
@Table(name = "orders", uniqueConstraints = {
        @UniqueConstraint(name = "uq_order_id",   columnNames = "order_id"),
        @UniqueConstraint(name = "uq_order_idem",  columnNames = "idempotency_key")
})
public class OrderEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id",        nullable = false) private String orderId;
    @Column(name = "user_id",         nullable = false) private String userId;
    @Column(name = "idempotency_key", nullable = false) private String idempotencyKey;
    @Enumerated(EnumType.STRING) private OrderType type;
    private String symbol;
    @Column(precision = 19, scale = 6) private BigDecimal quantity;
    @Column(precision = 19, scale = 4) private BigDecimal price;
    @Enumerated(EnumType.STRING) @Column(nullable = false) private OrderStatus status;
    @Column(name = "updated_at", nullable = false) private Instant updatedAt;

    protected OrderEntity() {}

    public OrderEntity(String orderId, String userId, String idempotencyKey,
                       OrderType type, String symbol,
                       BigDecimal quantity, BigDecimal price, OrderStatus status) {
        this.orderId = orderId; this.userId = userId;
        this.idempotencyKey = idempotencyKey; this.type = type;
        this.symbol = symbol; this.quantity = quantity;
        this.price = price; this.status = status;
        this.updatedAt = Instant.now();
    }

    public void transitionTo(OrderStatus next) {
        if (!status.canTransitionTo(next))
            throw new IllegalStateException(
                    "Illegal transition %s->%s for order %s".formatted(status, next, orderId));
        this.status = next;
        this.updatedAt = Instant.now();
    }

    public Long getId()        { return id; }
    public String getOrderId() { return orderId; }
    public String getUserId()  { return userId; }
    public OrderStatus getStatus() { return status; }
    public Instant getUpdatedAt()  { return updatedAt; }

    public OrderType getType()     { return type; }
    public String getSymbol()      { return symbol; }
    public BigDecimal getQuantity(){ return quantity; }
    public BigDecimal getPrice()   { return price; }

}
