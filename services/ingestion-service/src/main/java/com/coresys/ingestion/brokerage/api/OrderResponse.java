package com.coresys.ingestion.brokerage.api;

public record OrderResponse(String orderId, String status, String message) {}
