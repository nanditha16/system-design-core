package com.coresys.ingestion.banking.api;

public record PaymentResponse(String paymentId, String status, String message) {}
