package com.coresys.ingestion.feature.publish;

import com.coresys.common.events.TransactionEvent;
import com.coresys.common.events.Topics;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Idempotent producer (enable.idempotence=true, acks=all in application.yml).
 * Key = transactionId -> per-transaction ordering within a partition.
 */
@Service
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, TransactionEvent> kafka;

    public KafkaEventPublisher(KafkaTemplate<String, TransactionEvent> kafka) {
        this.kafka = kafka;
    }

    @Override
    public Mono<TransactionEvent> publish(TransactionEvent event) {
        return Mono.fromFuture(
                kafka.send(Topics.TRANSACTIONS_INCOMING, event.transactionId(), event)
                     .thenApply(result -> event));
    }
}
