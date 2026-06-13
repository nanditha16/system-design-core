package com.coresys.state.domain;

import com.coresys.common.events.TransactionEvent;
import com.coresys.common.events.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Single-writer state owner. One ACID transaction wraps:
 *   (1) eventId dedup insert  (2) state upsert / transition
 * Combined with offset-commit-after-write in the consumer, this yields the
 * standard exactly-once approximation: at-least-once delivery + idempotent apply.
 */
@Service
public class TransactionStateService {

    private static final Logger log = LoggerFactory.getLogger(TransactionStateService.class);

    private final TransactionRepository transactions;
    private final ProcessedEventRepository processedEvents;

    public TransactionStateService(TransactionRepository transactions,
                                   ProcessedEventRepository processedEvents) {
        this.transactions = transactions;
        this.processedEvents = processedEvents;
    }

    @Transactional
    public void apply(TransactionEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate event {} skipped (already applied)", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEventEntity(event.eventId()));

        transactions.findByTransactionId(event.transactionId())
                .ifPresentOrElse(
                        existing -> existing.transitionTo(event.status()),
                        () -> transactions.save(new TransactionEntity(
                                event.transactionId(), event.idempotencyKey(), event.amount(),
                                event.currency(), event.region(), event.type(), event.status())));

        // Demo completion: SENT events settle to SUCCESS immediately.
        // Real systems: settlement callback / downstream confirmation drives this.
        if (event.status() == TransactionStatus.SENT) {
            transactions.findByTransactionId(event.transactionId())
                    .ifPresent(t -> t.transitionTo(TransactionStatus.SUCCESS));
        }
    }
}
