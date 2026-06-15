package com.coresys.ingestion.banking.api;

import com.coresys.common.events.banking.PaymentRegion;
import com.coresys.common.events.banking.PaymentType;
import java.math.BigDecimal;

public record PaymentRequest(
        String userId,
        BigDecimal amount,
        String currency,
        PaymentType type,
        PaymentRegion region
) {}
