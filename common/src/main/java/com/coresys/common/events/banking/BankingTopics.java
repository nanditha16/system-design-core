package com.coresys.common.events.banking;

/**
 * Topic naming convention: <domain>.<flow>.v<version>
 *
 * Consumer groups (fan-out pattern):
 *   payment-events topic has 3 consumer groups:
 *   1. banking-state-manager  -> updates DB state
 *   2. banking-recovery-engine -> handles failures + retries
 *   3. banking-audit-sink     -> writes to S3/HDFS data lake
 */
public final class BankingTopics {
    private BankingTopics() {}
    public static final String PAYMENTS_INCOMING    = "banking.payments.incoming.v1";
    public static final String PAYMENTS_ROUTED      = "banking.payments.routed.v1";
    public static final String PAYMENT_EVENTS       = "banking.payment.events.v1";   // fan-out topic
    public static final String PAYMENTS_DLQ         = "banking.payments.dlq.v1";
    public static final String PAYMENTS_RETRY       = "banking.payments.retry.v1";
    public static final String RECON_DISCREPANCIES  = "banking.recon.discrepancies.v1";
}
