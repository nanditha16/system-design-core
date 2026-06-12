package com.coresys.ingestion.feature.publish;

import com.coresys.common.events.TransactionEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * STRUCTURAL FLAG: simple-sync-mode.
 * features.async.enabled=false -> Kafka disappears, ingestion calls
 * processing-service over REST. Used when the interview problem does not
 * justify event-driven complexity.
 */
@Service
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "false")
public class SyncRestPublisher implements EventPublisher {

    private final WebClient client;

    public SyncRestPublisher(@Value("${downstream.processing-url:http://localhost:8082}") String baseUrl) {
        this.client = WebClient.create(baseUrl);
    }

    @Override
    public Mono<TransactionEvent> publish(TransactionEvent event) {
        return client.post()
                .uri("/internal/process")
                .bodyValue(event)
                .retrieve()
                .bodyToMono(TransactionEvent.class);
    }
}
