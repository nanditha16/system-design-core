package com.coresys.ingestion.brokerage.api;

import com.coresys.common.events.brokerage.OrderType;
import java.math.BigDecimal;

public record OrderRequest(
        String userId,
        OrderType type,
        String symbol,
        BigDecimal quantity,
        BigDecimal price
) {}
