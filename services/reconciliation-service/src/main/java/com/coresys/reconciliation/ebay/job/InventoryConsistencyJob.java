package com.coresys.reconciliation.ebay.job;

import com.coresys.common.events.ebay.EbayTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Inventory Consistency Reconciliation Job.
 *
 * Detects two classes of problems:
 *
 * 1. STUCK_DRAFT: Listings that have been in DRAFT for > sla-seconds.
 *    Indicates enrichment pipeline failure (CompletableFuture timed out or failed).
 *    Action: publish to ebay.recon.v1 for re-trigger of enrichment.
 *
 * 2. INVENTORY_ZERO_ACTIVE: Listings in ACTIVE status with 0 available inventory
 *    that haven't been marked OUT_OF_STOCK.
 *    Indicates a race condition between inventory consumer and listing status update.
 *    Action: publish correction event.
 *
 * Mirrors the pattern of EodReconciliationJob (banking) and FeedConsistencyJob (instagram).
 * Source of truth: DB. Recovery: replay events from ebay.listings.incoming.v1.
 */
@Profile("ebay")
@Component
public class InventoryConsistencyJob {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsistencyJob.class);

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, Object> kafka;
    private final long slaSec;

    public InventoryConsistencyJob(
            JdbcTemplate jdbc,
            KafkaTemplate<String, Object> kafka,
            @Value("${recon.ebay.sla-seconds:300}") long slaSec) {
        this.jdbc = jdbc; this.kafka = kafka; this.slaSec = slaSec;
    }

    @Scheduled(fixedDelayString = "${recon.ebay.interval-ms:60000}")
    public void run() {
        Instant cutoff = Instant.now().minus(slaSec, ChronoUnit.SECONDS);
        log.info("eBay Inventory Consistency check. DRAFT SLA cutoff: {}", cutoff);

        // 1. Detect STUCK_DRAFT — listings stuck in DRAFT longer than SLA
        List<Map<String, Object>> stuckDrafts = jdbc.queryForList("""
                SELECT listing_id, seller_id, created_at
                FROM ebay_listing
                WHERE status = 'DRAFT'
                  AND created_at <= ?
                LIMIT 100
                """, java.sql.Timestamp.from(cutoff));

        if (!stuckDrafts.isEmpty()) {
            log.warn("eBay Recon: {} STUCK_DRAFT listings detected", stuckDrafts.size());
            stuckDrafts.forEach(row -> {
                String listingId = (String) row.get("listing_id");
                kafka.send(EbayTopics.RECON, listingId, Map.of(
                        "type",      "STUCK_DRAFT",
                        "listingId", listingId,
                        "sellerId",  row.get("seller_id"),
                        "stuckSince", row.get("created_at").toString(),
                        "detectedAt", Instant.now().toString()
                ));
                log.warn("STUCK_DRAFT: listingId={}", listingId);
            });
        }

        // 2. Detect INVENTORY_ZERO_ACTIVE — active listings with 0 inventory not marked out-of-stock
        List<Map<String, Object>> zeroInventory = jdbc.queryForList("""
                SELECT l.listing_id, l.seller_id, i.available_qty
                FROM ebay_listing l
                JOIN ebay_inventory i ON l.listing_id = i.listing_id
                WHERE l.status = 'ACTIVE'
                  AND i.available_qty = 0
                LIMIT 100
                """);

        if (!zeroInventory.isEmpty()) {
            log.warn("eBay Recon: {} INVENTORY_ZERO_ACTIVE listings detected", zeroInventory.size());
            zeroInventory.forEach(row -> {
                String listingId = (String) row.get("listing_id");
                kafka.send(EbayTopics.RECON, listingId, Map.of(
                        "type",      "INVENTORY_ZERO_ACTIVE",
                        "listingId", listingId,
                        "sellerId",  row.get("seller_id"),
                        "detectedAt", Instant.now().toString()
                ));
            });
        }

        if (stuckDrafts.isEmpty() && zeroInventory.isEmpty()) {
            log.info("eBay Inventory Consistency CLEAN: no discrepancies found");
        } else {
            log.warn("eBay Recon: published {} discrepancies to {}",
                    stuckDrafts.size() + zeroInventory.size(), EbayTopics.RECON);
        }
    }
}
