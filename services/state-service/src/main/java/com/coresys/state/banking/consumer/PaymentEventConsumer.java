package com.coresys.state.banking.consumer;

import com.coresys.common.events.banking.BankingTopics;
import com.coresys.common.events.banking.PaymentEvent;
import com.coresys.state.banking.service.PaymentStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * CONSUMER GROUP 2: State Manager
 * Consumes webhook results (CONFIRMED/FAILED transitions) from banking.payment.events.v1
 */
@Component
@Profile("banking")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);
    private final PaymentStateService stateService;

    public PaymentEventConsumer(PaymentStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = BankingTopics.PAYMENT_EVENTS, groupId = "banking-state-manager")
    public void onEvent(PaymentEvent event, Acknowledgment ack) {
        try {
            stateService.apply(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Event apply failed payment={}: {}", event.paymentId(), e.getMessage(), e);
            throw e;
        }
    }
}
