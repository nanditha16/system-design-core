package com.coresys.state.brokerage.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "holdings", uniqueConstraints = {
    @UniqueConstraint(name = "uq_holding", columnNames = {"user_id", "symbol"})
})
public class HoldingEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "user_id",  nullable = false) private String userId;
    @Column(nullable = false)                    private String symbol;
    @Column(nullable = false, precision = 19, scale = 6) private BigDecimal quantity;
    @Column(name = "updated_at", nullable = false)       private Instant updatedAt;

    protected HoldingEntity() {}

    public HoldingEntity(String userId, String symbol, BigDecimal quantity) {
        this.userId = userId; this.symbol = symbol;
        this.quantity = quantity; this.updatedAt = Instant.now();
    }

    public void addShares(BigDecimal qty) {
        this.quantity = this.quantity.add(qty);
        this.updatedAt = Instant.now();
    }

    public void removeShares(BigDecimal qty) {
        if (this.quantity.compareTo(qty) < 0)
            throw new IllegalStateException(
                "Insufficient shares: held=%s required=%s symbol=%s user=%s"
                    .formatted(this.quantity, qty, symbol, userId));
        this.quantity = this.quantity.subtract(qty);
        this.updatedAt = Instant.now();
    }

    public String getUserId()       { return userId; }
    public String getSymbol()       { return symbol; }
    public BigDecimal getQuantity() { return quantity; }
    public Instant getUpdatedAt()   { return updatedAt; }
}
