package com.coresys.ingestion.feature.idempotency;

import reactor.core.publisher.Mono;

/**
 * FEATURE MODULE: redis-idempotency
 * Abstraction boundary. Swap implementations via `features.idempotency.enabled`.
 * Interview narrative: "API-level dedup happens here, before anything hits Kafka."
 */
public interface IdempotencyService {
    /** @return true if the key was reserved now (first time seen), false if duplicate */
    Mono<Boolean> reserve(String idempotencyKey);
}
