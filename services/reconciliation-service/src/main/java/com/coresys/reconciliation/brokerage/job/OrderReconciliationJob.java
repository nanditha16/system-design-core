package com.coresys.reconciliation.brokerage.job;

import com.coresys.common.events.brokerage.BrokerageTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Uses native SQL — reconciliation-service has no JPA mapping for OrderEntity.
 * It reads the orders table directly as a read-only observer.
 * Interview point: "reconciliation reads a replica in production so batch
 * scans never contend with the hot write path in state-service."
 */
@Component
public class OrderReconciliationJob {

    private static final Logger log = LoggerFactory.getLogger(OrderReconciliationJob.class);

    private final EntityManager em;
    private final KafkaTemplate<String, Object> kafka;
    private final Duration slaWindow;

    public OrderReconciliationJob(EntityManager em,
                                  KafkaTemplate<String, Object> kafka,
                                  @Value("${recon.order.sla-seconds:60}") long slaSeconds) {
        this.em = em;
        this.kafka = kafka;
        this.slaWindow = Duration.ofSeconds(slaSeconds);
    }

    @Scheduled(fixedDelayString = "${recon.order.interval-ms:30000}")
    public void reconcile() {
        Instant cutoff = Instant.now().minus(slaWindow);

        @SuppressWarnings("unchecked")
        List<Object[]> stuck = em.createNativeQuery(
                "SELECT order_id, user_id, status, updated_at " +
                "FROM orders " +
                "WHERE status IN ('PENDING','EXECUTING') AND updated_at < :cutoff")
                .setParameter("cutoff", java.sql.Timestamp.from(cutoff))
                .getResultList();

        if (stuck.isEmpty()) {
            log.info("Brokerage reconciliation clean: no stuck orders");
            return;
        }

        log.warn("Brokerage recon found {} stuck orders", stuck.size());
        stuck.forEach(row -> kafka.send(
                BrokerageTopics.RECON_DISCREPANCIES,
                (String) row[0],
                Map.of("orderId",    row[0],
                       "userId",     row[1],
                       "status",     row[2],
                       "stuckSince", row[3].toString(),
                       "detectedAt", Instant.now().toString())));
    }
}
