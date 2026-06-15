package com.coresys.state.banking.domain;

import com.coresys.common.events.banking.PaymentRegion;
import com.coresys.common.events.banking.PaymentStatus;
import com.coresys.common.events.banking.PaymentType;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Source of truth for payment state.
 * UNIQUE constraints are the last-resort exactly-once backstop.
 */
@Entity
@Table(name = "banking_payments", uniqueConstraints = {
        @UniqueConstraint(name = "uq_payment_id",   columnNames = "payment_id"),
        @UniqueConstraint(name = "uq_payment_idem",  columnNames = "idempotency_key")
})
public class PaymentEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "payment_id",      nullable = false) private String paymentId;
    @Column(name = "idempotency_key", nullable = false) private String idempotencyKey;
    @Column(name = "user_id",         nullable = false) private String userId;
    @Column(nullable = false, precision = 19, scale = 4) private BigDecimal amount;
    private String currency;
    @Enumerated(EnumType.STRING) private PaymentType type;
    @Enumerated(EnumType.STRING) private PaymentRegion region;
    @Enumerated(EnumType.STRING) @Column(nullable = false) private PaymentStatus status;
    @Column(name = "core_banking_ref") private String coreBankingRef;
    @Column(name = "created_at",  nullable = false) private Instant createdAt;
    @Column(name = "updated_at",  nullable = false) private Instant updatedAt;

    protected PaymentEntity() {}

    public PaymentEntity(String paymentId, String idempotencyKey, String userId,
                         BigDecimal amount, String currency,
                         PaymentType type, PaymentRegion region, PaymentStatus status) {
        this.paymentId = paymentId; this.idempotencyKey = idempotencyKey;
        this.userId = userId; this.amount = amount; this.currency = currency;
        this.type = type; this.region = region; this.status = status;
        this.createdAt = Instant.now(); this.updatedAt = Instant.now();
    }

    public void transitionTo(PaymentStatus next, String coreBankingRef) {
        if (!status.canTransitionTo(next))
            throw new IllegalStateException(
                    "Illegal transition %s->%s for payment %s".formatted(status, next, paymentId));
        this.status = next;
        if (coreBankingRef != null) this.coreBankingRef = coreBankingRef;
        this.updatedAt = Instant.now();
    }

    public Long getId()                { return id; }
    public String getPaymentId()       { return paymentId; }
    public String getIdempotencyKey()  { return idempotencyKey; }
    public String getUserId()          { return userId; }
    public BigDecimal getAmount()      { return amount; }
    public String getCurrency()        { return currency; }
    public PaymentType getType()       { return type; }
    public PaymentRegion getRegion()   { return region; }
    public PaymentStatus getStatus()   { return status; }
    public String getCoreBankingRef()  { return coreBankingRef; }
    public Instant getCreatedAt()      { return createdAt; }
    public Instant getUpdatedAt()      { return updatedAt; }
}
