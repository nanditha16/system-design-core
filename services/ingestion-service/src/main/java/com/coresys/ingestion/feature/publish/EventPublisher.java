package com.coresys.ingestion.feature.publish;

import com.coresys.common.events.TransactionEvent;
import reactor.core.publisher.Mono;

/**
 * FEATURE MODULE: kafka-enabled vs simple-sync-mode
 * Same controller, two transport strategies:
 *  - KafkaEventPublisher  (event-driven, default)
 *  - SyncRestPublisher    (direct REST call, for simple orchestration designs)
 */
public interface EventPublisher {
    Mono<TransactionEvent> publish(TransactionEvent event);
}
