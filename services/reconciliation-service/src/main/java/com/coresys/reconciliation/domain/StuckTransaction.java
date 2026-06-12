package com.coresys.reconciliation.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Read-only projection over the transactions table owned by state-service.
 * NOTE (interview talking point): locally we share one Postgres for simplicity;
 * in production reconciliation reads a replica / CDC-fed read model so the
 * batch scan never contends with the hot write path.
 */
@Entity
@Table(name = "transactions")
public class StuckTransaction {

    @Id
    private Long id;

    @Column(name = "transaction_id")
    private String transactionId;

    private String status;

    @Column(name = "updated_at")
    private Instant updatedAt;

    public Long getId() { return id; }
    public String getTransactionId() { return transactionId; }
    public String getStatus() { return status; }
    public Instant getUpdatedAt() { return updatedAt; }
}
