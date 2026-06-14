package com.coresys.common.events.brokerage;

public final class BrokerageTopics {
    private BrokerageTopics() {}
    public static final String ORDERS_INCOMING     = "orders.incoming.v1";
    public static final String ORDERS_PROCESSED    = "orders.processed.v1";
    public static final String ORDERS_DLQ          = "orders.dlq.v1";
    public static final String RECON_DISCREPANCIES = "orders.discrepancies.v1";
    public static final String ORDERS_REJECTED     = "orders.rejected.v1";
}
