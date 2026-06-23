package com.coresys.common.events.ebay;

import java.time.Instant;

public record SellerEvent(
        String eventId,
        String sellerId,
        String businessName,
        String email,
        String countryCode,
        String action,          // ONBOARDED | ACTIVATED | SUSPENDED
        Instant occurredAt
) {}
