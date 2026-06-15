package com.coresys.ingestion.feature.publish;

import com.coresys.common.events.banking.BankingTopics;
import com.coresys.common.events.banking.PaymentEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Two publish methods:
 *  publish()      -> payments.incoming (new payment from client)
 *  publishEvent() -> payment.events    (webhook callback from core banking)
 */
@Service
public class BankingEventPublisher {

    private final KafkaTemplate<String, Object> kafka;

    public BankingEventPublisher(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    public Mono<PaymentEvent> publish(PaymentEvent event) {
        return Mono.fromFuture(
                kafka.send(BankingTopics.PAYMENTS_INCOMING, event.paymentId(), event)
                     .thenApply(r -> event));
    }

    public Mono<PaymentEvent> publishEvent(PaymentEvent event) {
        return Mono.fromFuture(
                kafka.send(BankingTopics.PAYMENT_EVENTS, event.paymentId(), event)
                     .thenApply(r -> event));
    }
}
