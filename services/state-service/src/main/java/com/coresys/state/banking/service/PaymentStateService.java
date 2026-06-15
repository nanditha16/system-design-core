package com.coresys.state.banking.service;

import com.coresys.common.events.banking.PaymentEvent;
import com.coresys.common.events.banking.PaymentStatus;
import com.coresys.state.banking.domain.PaymentEntity;
import com.coresys.state.banking.domain.PaymentRepository;
import com.coresys.state.domain.ProcessedEventEntity;
import com.coresys.state.domain.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * CONSUMER GROUP 2: State Manager
 *
 * Handles two event types from banking.payment.events.v1:
 *  - SENT events (from router): PENDING -> SENT
 *  - CONFIRMED/FAILED events (from webhook): SENT -> CONFIRMED/FAILED
 *
 * One ACID transaction = eventId dedup + state transition.
 * Offset committed AFTER DB write (in consumer).
 */
@Service
public class PaymentStateService {

    private static final Logger log = LoggerFactory.getLogger(PaymentStateService.class);

    private final PaymentRepository payments;
    private final ProcessedEventRepository processedEvents;

    public PaymentStateService(PaymentRepository payments,
                               ProcessedEventRepository processedEvents) {
        this.payments = payments;
        this.processedEvents = processedEvents;
    }

    @Transactional
    public void apply(PaymentEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate event {} skipped", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEventEntity(event.eventId()));

        try {
            payments.findByPaymentId(event.paymentId())
                    .ifPresentOrElse(
                        existing -> {
                            existing.transitionTo(event.status(), event.coreBankingRef());
                            log.info("Payment state: {} -> {} ref={}",
                                    event.paymentId(), event.status(), event.coreBankingRef());
                        },
                        () -> {
                            // First time seen: create PENDING record
                            payments.save(new PaymentEntity(
                                    event.paymentId(), event.idempotencyKey(), event.userId(),
                                    event.amount(), event.currency(),
                                    event.type(), event.region(), event.status()));
                            log.info("Payment created: {} status={}", event.paymentId(), event.status());
                        });

        } catch (DataIntegrityViolationException e) {
            log.warn("Idempotent skip payment={}: {}", event.paymentId(), e.getMessage());
        }
    }
}
