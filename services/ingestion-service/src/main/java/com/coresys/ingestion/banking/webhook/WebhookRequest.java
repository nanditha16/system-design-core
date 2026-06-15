package com.coresys.ingestion.banking.webhook;

public record WebhookRequest(
        String paymentId,
        String coreBankingRef,
        String result,   // SUCCESS | FAILURE
        String reason
) {}
