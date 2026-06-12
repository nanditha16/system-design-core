package com.coresys.common.events;

/** Single source of truth for Kafka topic names. */
public final class Topics {
    private Topics() {}
    public static final String TRANSACTIONS_INCOMING  = "transactions.incoming.v1";
    public static final String TRANSACTIONS_PROCESSED = "transactions.processed.v1";
    public static final String TRANSACTIONS_DLQ       = "transactions.dlq.v1";
    public static final String RECON_DISCREPANCIES    = "reconciliation.discrepancies.v1";
}
