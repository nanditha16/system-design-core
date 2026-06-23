package com.coresys.common.events.ebay;

/**
 * eBay Seller Platform topic layout (mirrors instagram pattern):
 *
 * listings.incoming.v1  → new listing from seller (POST /listings)
 * listings.enriched.v1  → after parallel enrichment (category/tax/compliance/image)
 * inventory.events.v1   → ADD / RESERVE / SELL / RELEASE inventory adjustments
 * seller.events.v1      → seller onboarded / activated
 * ebay.dlq.v1           → dead letter queue
 * ebay.recon.v1         → inventory consistency discrepancies
 */
public final class EbayTopics {
    private EbayTopics() {}
    public static final String LISTINGS_INCOMING  = "ebay.listings.incoming.v1";
    public static final String LISTINGS_ENRICHED  = "ebay.listings.enriched.v1";
    public static final String INVENTORY_EVENTS   = "ebay.inventory.events.v1";
    public static final String SELLER_EVENTS      = "ebay.seller.events.v1";
    public static final String DLQ                = "ebay.dlq.v1";
    public static final String RECON              = "ebay.recon.v1";
}
