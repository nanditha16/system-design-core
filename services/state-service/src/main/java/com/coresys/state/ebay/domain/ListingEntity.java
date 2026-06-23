package com.coresys.state.ebay.domain;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * PARTITION STRATEGY: hash(listing_id), NOT seller_id.
 *
 * Why: enterprise sellers (eBay Business) may have millions of listings.
 * Partitioning by seller_id creates a hotspot — all writes for that seller
 * hit the same shard. listing_id is UUID → uniformly distributed across shards.
 *
 * DB index on seller_id still exists for "get all listings by seller" queries.
 * The index is on the shard that owns the listing, so the query fans out
 * across shards — acceptable for O(1) dashboard reads (pre-computed in Redis).
 */
@Entity
@Table(name = "ebay_listing", indexes = {
        @Index(name = "idx_ebay_listing_seller", columnList = "seller_id"),
        @Index(name = "idx_ebay_listing_status", columnList = "status")
}, uniqueConstraints = {
        @UniqueConstraint(name = "uq_ebay_listing_idem", columnNames = "idempotency_key")
})
public class ListingEntity {
    @Id @Column(name = "listing_id")             private String listingId;
    @Column(name = "seller_id", nullable = false) private String sellerId;
    @Column(name = "product_id")                 private String productId;
    @Column(precision = 19, scale = 4)           private BigDecimal price;
    @Column(length = 3)                          private String currency;
    @Column(nullable = false)                    private String status;
    @Column(length = 2)                          private String region;
    @Column(name = "idempotency_key")            private String idempotencyKey;
    @Column(name = "tax_rate")                   private double taxRate;
    @Column(name = "processed_image_url")        private String processedImageUrl;
    @Column(name = "created_at")                 private Instant createdAt;
    @Column(name = "updated_at")                 private Instant updatedAt;

    protected ListingEntity() {}
    public ListingEntity(String listingId, String sellerId, String productId,
                         BigDecimal price, String currency, String region,
                         String idempotencyKey) {
        this.listingId = listingId; this.sellerId = sellerId; this.productId = productId;
        this.price = price; this.currency = currency; this.region = region;
        this.idempotencyKey = idempotencyKey; this.status = "DRAFT";
        this.createdAt = Instant.now(); this.updatedAt = Instant.now();
    }
    public String getListingId()         { return listingId; }
    public String getSellerId()          { return sellerId; }
    public String getProductId()         { return productId; }
    public BigDecimal getPrice()         { return price; }
    public String getCurrency()          { return currency; }
    public String getStatus()            { return status; }
    public String getRegion()            { return region; }
    public String getIdempotencyKey()    { return idempotencyKey; }
    public double getTaxRate()           { return taxRate; }
    public String getProcessedImageUrl() { return processedImageUrl; }
    public Instant getCreatedAt()        { return createdAt; }
    public Instant getUpdatedAt()        { return updatedAt; }
    public void activate(double taxRate, String imgUrl) {
        this.status = "ACTIVE"; this.taxRate = taxRate;
        this.processedImageUrl = imgUrl; this.updatedAt = Instant.now();
    }
    public void markOutOfStock() { this.status = "OUT_OF_STOCK"; this.updatedAt = Instant.now(); }
}
