package com.coresys.ingestion.ebay.api;
import java.math.BigDecimal;
public record CreateListingInput(
        String sellerId, String productId,
        BigDecimal price, String currency,
        String region, int initialQty) {}
