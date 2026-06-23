package com.coresys.ingestion.ebay.api;
public record InventoryAdjustmentInput(
        String listingId, String eventType,
        int quantity, String orderId) {}
