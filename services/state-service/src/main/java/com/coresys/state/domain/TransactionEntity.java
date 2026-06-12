package com.coresys.state.domain;

import com.coresys.common.events.TransactionStatus;
import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * DB-level exactly-once backstop:
 *  - UNIQUE(transaction_id)  -> a duplicate event can never create a second row
 *  - UNIQUE(idempotency_key) -> a duplicate API call can never create a second txn
 */
@Entity
@Table(name = "transactions", uniqueConstraints = {
        @UniqueConstraint(name = "uq_txn_id", columnNames = "transactionId"),
        @UniqueConstraint(name = "uq_idem_key", columnNames = "idempotencyKey")
})
public class TransactionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String transactionId;

    @Column(nullable = false)
    private String idempotencyKey;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    private String currency;
    private String region;
    private String type;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TransactionStatus status;

    @Column(nullable = false)
    private Instant updatedAt;

    protected TransactionEntity() {}

    public TransactionEntity(String transactionId, String idempotencyKey, BigDecimal amount,
                             String currency, String region, String type, TransactionStatus status) {
        this.transactionId = transactionId;
        this.idempotencyKey = idempotencyKey;
        this.amount = amount;
        this.currency = currency;
        this.region = region;
        this.type = type;
        this.status = status;
        this.updatedAt = Instant.now();
    }

    /** State machine enforcement: illegal transitions throw, terminal states are immutable. */
    public void transitionTo(TransactionStatus next) {
        if (!status.canTransitionTo(next)) {
            throw new IllegalStateException(
                    "Illegal transition %s -> %s for txn %s".formatted(status, next, transactionId));
        }
        this.status = next;
        this.updatedAt = Instant.now();
    }

    public Long getId() { return id; }
    public String getTransactionId() { return transactionId; }
    public TransactionStatus getStatus() { return status; }
    public Instant getUpdatedAt() { return updatedAt; }
}
