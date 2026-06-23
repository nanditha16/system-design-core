package com.coresys.state.ebay.api;

import com.coresys.state.ebay.domain.*;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Profile("ebay")
@RestController
@RequestMapping("/api/v1/ebay/listings")
public class ListingQueryController {

    private final ListingRepository listings;
    private final InventoryRepository inventory;

    public ListingQueryController(ListingRepository listings, InventoryRepository inventory) {
        this.listings = listings; this.inventory = inventory;
    }

    @GetMapping("/{listingId}")
    public ResponseEntity<ListingEntity> getListing(@PathVariable String listingId) {
        return listings.findById(listingId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/seller/{sellerId}")
    public List<ListingEntity> getBySellerAndStatus(
            @PathVariable String sellerId,
            @RequestParam(required = false) String status) {
        return status != null
                ? listings.findBySellerIdAndStatus(sellerId, status)
                : listings.findBySellerIdOrderByCreatedAtDesc(sellerId);
    }

    @GetMapping("/{listingId}/inventory")
    public ResponseEntity<Map<String, Object>> getInventory(@PathVariable String listingId) {
        return inventory.findById(listingId)
                .map(inv -> {
                    Map<String, Object> body = new java.util.HashMap<>();
                    body.put("listingId",    inv.getListingId());
                    body.put("availableQty", inv.getAvailableQty());
                    body.put("reservedQty",  inv.getReservedQty());
                    body.put("soldQty",      inv.getSoldQty());
                    body.put("version",      inv.getVersion());
                    return ResponseEntity.<Map<String, Object>>ok(body);
                })
                .orElse(ResponseEntity.notFound().build());
    }
}
