package com.coresys.processing.banking.consumer;

import com.coresys.common.events.banking.BankingTopics;
import com.coresys.common.events.banking.PaymentEvent;
import com.coresys.processing.banking.routing.PaymentRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@Profile("banking")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private final PaymentRouter router;
    private final KafkaTemplate<String, Object> kafka;

    public PaymentConsumer(PaymentRouter router, KafkaTemplate<String, Object> kafka) {
        this.router = router;
        this.kafka = kafka;
    }

    @KafkaListener(topics = BankingTopics.PAYMENTS_INCOMING, groupId = "banking-router")
    public void onPayment(PaymentEvent event, Acknowledgment ack) {
        try {
            PaymentEvent routed = router.route(event).block();
            kafka.send(BankingTopics.PAYMENTS_ROUTED, routed.paymentId(), routed).join();
            ack.acknowledge();
            log.info("Payment routed: {} {} -> coreBankingRef={}",
                    routed.paymentId(), routed.status(), routed.coreBankingRef());
        } catch (Exception e) {
            log.error("Payment routing failed {}: {}", event.paymentId(), e.getMessage(), e);
            throw e;
        }
    }
}