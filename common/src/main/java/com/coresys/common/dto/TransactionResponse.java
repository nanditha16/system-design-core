package com.coresys.common.dto;

public record TransactionResponse(
        String transactionId,
        String status,
        String message
) {}
