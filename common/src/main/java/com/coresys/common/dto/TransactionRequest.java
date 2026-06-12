package com.coresys.common.dto;

import java.math.BigDecimal;

public record TransactionRequest(
        String accountId,
        BigDecimal amount,
        String currency,
        String region,
        String type
) {}
