package com.coresys.ingestion.feature.idempotency;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * STRUCTURAL FLAG OFF: simple-sync-mode / no-redis scenarios.
 * Set features.idempotency.enabled=false and Redis disappears from the design
 * without touching the controller.
 */
@Service
@ConditionalOnProperty(name = "features.idempotency.enabled", havingValue = "false")
public class NoOpIdempotencyService implements IdempotencyService {
    @Override
    public Mono<Boolean> reserve(String idempotencyKey) {
        return Mono.just(true);
    }
}
