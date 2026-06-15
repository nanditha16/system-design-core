package com.coresys.reconciliation.job;

import com.coresys.common.events.Topics;
import com.coresys.reconciliation.domain.StuckTransaction;
import com.coresys.reconciliation.domain.StuckTransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * FEATURE MODULE: reconciliation-batch (simulated EOD reconciliation).
 * Detects transactions stuck in non-terminal states beyond SLA and publishes
 * discrepancy events for ops / audit consumers.
 *
 * Interview variants:
 *  - EOD batch: replace fixedDelay with cron "0 0 2 * * *"
 *  - External recon: join against settlement-provider file (external-integration module)
 */
@Component
@Profile("generic")
public class ReconciliationJob {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationJob.class);

    private final StuckTransactionRepository repository;
    private final KafkaTemplate<String, Object> kafka;
    private final Duration slaWindow;

    public ReconciliationJob(StuckTransactionRepository repository,
                             KafkaTemplate<String, Object> kafka,
                             @Value("${recon.sla-seconds:60}") long slaSeconds) {
        this.repository = repository;
        this.kafka = kafka;
        this.slaWindow = Duration.ofSeconds(slaSeconds);
    }

    @Scheduled(fixedDelayString = "${recon.interval-ms:30000}")
    public void reconcile() {
        Instant cutoff = Instant.now().minus(slaWindow);
        List<StuckTransaction> stuck = repository.findStuck(cutoff);

        if (stuck.isEmpty()) {
            log.info("Reconciliation pass clean: no stuck transactions");
            return;
        }

        log.warn("Reconciliation found {} stuck transactions", stuck.size());
        stuck.forEach(t -> kafka.send(Topics.RECON_DISCREPANCIES, t.getTransactionId(),
                Map.of("transactionId", t.getTransactionId(),
                       "status", t.getStatus(),
                       "stuckSince", t.getUpdatedAt().toString(),
                       "detectedAt", Instant.now().toString())));
    }
}
