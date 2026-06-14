package com.coresys.state.brokerage.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Table(name = "accounts")
public class AccountEntity {

    @Id
    @Column(name = "user_id")
    private String userId;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal balance;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected AccountEntity() {}

    public AccountEntity(String userId, BigDecimal initialBalance) {
        this.userId = userId;
        this.balance = initialBalance;
        this.updatedAt = Instant.now();
    }

    /**
     * BUY path: debit.
     * Throws IllegalStateException on insufficient funds.
     * Caller is inside @Transactional -> rolls back order write atomically.
     */
    public void debit(BigDecimal amount) {
        if (balance.compareTo(amount) < 0)
            throw new IllegalStateException(
                    "Insufficient funds: balance=%s required=%s user=%s"
                            .formatted(balance, amount, userId));
        this.balance = balance.subtract(amount);
        this.updatedAt = Instant.now();
    }

    /** SELL path: credit. */
    public void credit(BigDecimal amount) {
        this.balance = balance.add(amount);
        this.updatedAt = Instant.now();
    }

    public String getUserId()  { return userId;  }
    public BigDecimal getBalance() { return balance; }
    public Instant getUpdatedAt()  { return updatedAt; }
}
