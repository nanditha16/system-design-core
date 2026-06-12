package com.coresys.ingestion.feature.idempotency;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Atomic SET-IF-ABSENT (SETNX) with TTL.
 * TTL bounds memory; window must exceed client retry horizon (24h default).
 */
@Service
@ConditionalOnProperty(name = "features.idempotency.enabled", havingValue = "true", matchIfMissing = true)
public class RedisIdempotencyService implements IdempotencyService {

    private static final String PREFIX = "idem:";
    private final ReactiveStringRedisTemplate redis;
    private final Duration ttl;

    public RedisIdempotencyService(ReactiveStringRedisTemplate redis,
                                   @Value("${features.idempotency.ttl-hours:24}") long ttlHours) {
        this.redis = redis;
        this.ttl = Duration.ofHours(ttlHours);
    }

    @Override
    public Mono<Boolean> reserve(String idempotencyKey) {
        return redis.opsForValue()
                .setIfAbsent(PREFIX + idempotencyKey, "1", ttl)
                .defaultIfEmpty(false);
    }
}
