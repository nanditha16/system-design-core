package com.coresys.state.ebay.api;

import com.coresys.state.ebay.domain.*;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Profile("ebay")
@RestController
@RequestMapping("/api/v1/ebay/sellers")
public class SellerQueryController {

    private final SellerRepository sellers;
    private final ListingRepository listings;
    private final InventoryRepository inventory;

    public SellerQueryController(SellerRepository sellers, ListingRepository listings,
                                  InventoryRepository inventory) {
        this.sellers = sellers; this.listings = listings; this.inventory = inventory;
    }

    @GetMapping("/{sellerId}")
    public ResponseEntity<SellerEntity> getSeller(@PathVariable String sellerId) {
        return sellers.findById(sellerId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Seller dashboard — pre-computed aggregate (Redis in production, live DB here).
     *
     * Production: O(1) Redis GET from dashboard:{sellerId} key.
     * Here: live DB count queries (acceptable for demo, not for 10M DAU).
     *
     * Interview talking point:
     * "SELECT COUNT(*) FROM ebay_listing over billions of rows = 10+ seconds.
     *  We maintain a SellerDashboardView in Redis, updated by CDC → Kafka → DashboardConsumer.
     *  Dashboard query becomes a single Redis GET — O(1) regardless of listing count."
     */
    @GetMapping("/{sellerId}/dashboard")
    public Map<String, Object> getDashboard(@PathVariable String sellerId) {
        long active     = listings.countBySellerIdAndStatus(sellerId, "ACTIVE");
        long paused     = listings.countBySellerIdAndStatus(sellerId, "PAUSED");
        long outOfStock = listings.countBySellerIdAndStatus(sellerId, "OUT_OF_STOCK");
        long draft      = listings.countBySellerIdAndStatus(sellerId, "DRAFT");

        List<String> activeIds = listings.findBySellerIdAndStatus(sellerId, "ACTIVE")
                .stream().map(ListingEntity::getListingId).toList();
        long totalInventory = inventory.findByListingIdIn(activeIds)
                .stream().mapToLong(InventoryEntity::getAvailableQty).sum();

                // In getDashboard, replace Map.of(...) with:
                Map<String, Object> dashboard = new java.util.HashMap<>();
                dashboard.put("sellerId",       sellerId);
                dashboard.put("activeListings", active);
                dashboard.put("pausedListings", paused);
                dashboard.put("outOfStock",     outOfStock);
                dashboard.put("draftListings",  draft);
                dashboard.put("totalInventory", totalInventory);
                return dashboard;
    }
}
