package com.coresys.state.ebay.domain;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * Seller account on the marketplace.
 * Partition: seller_id (UUID) — not a hotspot since seller writes are infrequent.
 */
@Entity
@Table(name = "ebay_seller",
       uniqueConstraints = @UniqueConstraint(name = "uq_ebay_seller_email", columnNames = "email"))
public class SellerEntity {
    @Id @Column(name = "seller_id")              private String sellerId;
    @Column(nullable = false, name = "business_name") private String businessName;
    @Column(nullable = false)                     private String email;
    @Column(name = "country_code", length = 2)   private String countryCode;
    @Column(nullable = false)                     private String status;
    @Column(name = "created_at")                 private Instant createdAt;
    @Column(name = "updated_at")                 private Instant updatedAt;

    protected SellerEntity() {}
    public SellerEntity(String sellerId, String businessName, String email, String countryCode) {
        this.sellerId = sellerId; this.businessName = businessName;
        this.email = email; this.countryCode = countryCode;
        this.status = "PENDING_VERIFICATION";
        this.createdAt = Instant.now(); this.updatedAt = Instant.now();
    }
    public String getSellerId()      { return sellerId; }
    public String getBusinessName()  { return businessName; }
    public String getEmail()         { return email; }
    public String getCountryCode()   { return countryCode; }
    public String getStatus()        { return status; }
    public Instant getCreatedAt()    { return createdAt; }
    public void activate()           { this.status = "ACTIVE"; this.updatedAt = Instant.now(); }
}
