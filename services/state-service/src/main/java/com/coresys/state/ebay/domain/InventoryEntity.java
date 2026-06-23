package com.coresys.state.ebay.domain;

import jakarta.persistence.*;

/**
 * Inventory is SEPARATE from Listing — critical design decision.
 *
 * Why separate:
 *   - Inventory changes on every order (RESERVE/SELL) → high write frequency
 *   - Listing metadata changes rarely (price edit, description update)
 *   - Mixing them forces inventory row locks to block listing reads
 *
 * @Version → JPA optimistic locking:
 *   UPDATE ebay_inventory SET available_qty=?, version=version+1
 *   WHERE listing_id=? AND version=?    ← the anti-oversell guard
 *   rows_affected=0 → ObjectOptimisticLockingFailureException → retry (max 3x)
 *
 * PARTITION: listing_id → same shard as ListingEntity for co-location
 */
@Entity
@Table(name = "ebay_inventory")
public class InventoryEntity {
    @Id @Column(name = "listing_id") private String listingId;
    @Column(name = "available_qty")  private int availableQty;
    @Column(name = "reserved_qty")   private int reservedQty;
    @Column(name = "sold_qty")       private int soldQty;
    @Version                         private long version;

    protected InventoryEntity() {}
    public InventoryEntity(String listingId, int initialQty) {
        this.listingId = listingId; this.availableQty = initialQty;
    }
    public String getListingId()  { return listingId; }
    public int getAvailableQty()  { return availableQty; }
    public int getReservedQty()   { return reservedQty; }
    public int getSoldQty()       { return soldQty; }
    public long getVersion()      { return version; }

    public void add(int qty)      { this.availableQty += qty; }
    public void reserve(int qty)  {
        if (availableQty < qty) throw new IllegalStateException("Insufficient stock");
        this.availableQty -= qty; this.reservedQty += qty;
    }
    public void release(int qty)  { this.reservedQty -= qty; this.availableQty += qty; }
    public void sell(int qty)     { this.reservedQty -= qty; this.soldQty += qty; }
}
