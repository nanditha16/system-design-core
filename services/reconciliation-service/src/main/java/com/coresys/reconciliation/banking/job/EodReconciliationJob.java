package com.coresys.reconciliation.banking.job;

import com.coresys.common.events.banking.BankingTopics;
import com.coresys.common.events.banking.PaymentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;

import jakarta.persistence.EntityManager;
import java.time.*;
import java.util.*;

/**
 * EOD Reconciliation Engine.
 *
 * Design: "Compare what we THINK happened (Gateway DB) vs what ACTUALLY happened (Core Banking)."
 *
 * In production this is a Spark/Flink job:
 *   1. Read Gateway data from S3 (PaymentCreated partition)
 *   2. Read Core Banking settlement file (S3 or SFTP)
 *   3. JOIN on payment_id + amount + currency
 *   4. Classify: MATCHED | MISSING_IN_CORE | MISSING_IN_GATEWAY
 *   5. Publish discrepancies to Kafka for alert/replay
 *
 * Locally: we simulate by:
 *   - Gateway data = banking_payments table (our DB)
 *   - Core Banking data = CONFIRMED records (mock: same DB, filtered by status)
 *   - Discrepancies = SENT records stuck past SLA (should have become CONFIRMED)
 *
 * Schedule: @Scheduled(cron = "0 0 2 * * MON-FRI") for real EOD (2am weekdays)
 */
@Component
@Profile("banking")
public class EodReconciliationJob {

    private static final Logger log = LoggerFactory.getLogger(EodReconciliationJob.class);

    private final EntityManager em;
    private final KafkaTemplate<String, Object> kafka;
    private final long slaSecs;

    public EodReconciliationJob(EntityManager em, KafkaTemplate<String, Object> kafka,
                                @Value("${recon.banking.sla-seconds:120}") long slaSecs) {
        this.em = em;
        this.kafka = kafka;
        this.slaSecs = slaSecs;
    }

    /**
     * Runs every 60 seconds locally (simulates intra-day recon).
     * Change to cron = "0 0 2 * * MON-FRI" for true EOD.
     */
    @Scheduled(fixedDelayString = "${recon.banking.interval-ms:60000}")
    public void reconcile() {
        Instant cutoff = Instant.now().minus(Duration.ofSeconds(slaSecs));
        log.info("EOD Reconciliation starting. SLA cutoff: {}", cutoff);

        // Case 1: MISSING_IN_CORE — payments SENT but not CONFIRMED past SLA
        @SuppressWarnings("unchecked")
        List<Object[]> missingInCore = em.createNativeQuery(
                "SELECT payment_id, user_id, amount, currency, type, region, updated_at " +
                "FROM banking_payments " +
                "WHERE status = 'SENT' AND updated_at < :cutoff")
                .setParameter("cutoff", java.sql.Timestamp.from(cutoff))
                .getResultList();

        // Case 2: PENDING too long — ingested but never routed
        @SuppressWarnings("unchecked")
        List<Object[]> stuckPending = em.createNativeQuery(
                "SELECT payment_id, user_id, amount, currency, updated_at " +
                "FROM banking_payments " +
                "WHERE status = 'PENDING' AND updated_at < :cutoff")
                .setParameter("cutoff", java.sql.Timestamp.from(cutoff))
                .getResultList();

        if (missingInCore.isEmpty() && stuckPending.isEmpty()) {
            log.info("EOD Reconciliation CLEAN: no discrepancies found");
            return;
        }

        log.warn("EOD Discrepancies: missingInCore={} stuckPending={}",
                missingInCore.size(), stuckPending.size());

        missingInCore.forEach(row -> {
            String payload = String.format(
                "{\"type\":\"MISSING_IN_CORE\",\"paymentId\":\"%s\",\"userId\":\"%s\"," +
                "\"amount\":\"%s\",\"currency\":\"%s\",\"paymentType\":\"%s\"," +
                "\"region\":\"%s\",\"stuckSince\":\"%s\",\"detectedAt\":\"%s\"}",
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], Instant.now());
            kafka.send(BankingTopics.RECON_DISCREPANCIES, (String) row[0], payload);
        });

        stuckPending.forEach(row -> {
            String payload = String.format(
                "{\"type\":\"STUCK_PENDING\",\"paymentId\":\"%s\",\"userId\":\"%s\"," +
                "\"amount\":\"%s\",\"currency\":\"%s\",\"stuckSince\":\"%s\",\"detectedAt\":\"%s\"}",
                row[0], row[1], row[2], row[3], row[4], Instant.now());
            kafka.send(BankingTopics.RECON_DISCREPANCIES, (String) row[0], payload);
        });

        log.info("EOD: Published {} discrepancy events to {}",
                missingInCore.size() + stuckPending.size(), BankingTopics.RECON_DISCREPANCIES);
    }
}
