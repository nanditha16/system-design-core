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

/** Consumes routed payments (PENDING->SENT transitions) */
@Component
@Profile("banking")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class PaymentRoutedConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentRoutedConsumer.class);
    private final PaymentStateService stateService;

    public PaymentRoutedConsumer(PaymentStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = BankingTopics.PAYMENTS_ROUTED, groupId = "banking-state-routed")
    public void onRouted(PaymentEvent event, Acknowledgment ack) {
        try {
            stateService.apply(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("State apply failed payment={}: {}", event.paymentId(), e.getMessage(), e);
            throw e;
        }
    }
}
