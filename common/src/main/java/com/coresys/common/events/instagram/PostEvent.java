package com.coresys.common.events.instagram;

import java.time.Instant;

/**
 * Canonical post event flowing through Kafka.
 * eventId   → consumer-side dedup key
 * postId    → business key, unique in DB
 * s3Key     → raw media location (served via CDN in production)
 * followerCount → used by FanoutRouter to choose strategy
 */
public record PostEvent(
        String eventId,
        String postId,
        String idempotencyKey,
        String authorId,
        String caption,
        String s3Key,           // s3://bucket/posts/{postId}/{filename}
        String cdnUrl,          // https://cdn.example.com/posts/{postId}/{filename}
        MediaType mediaType,
        long followerCount,
        FanoutStrategy fanoutStrategy,
        Instant occurredAt
) {
    public PostEvent withFanoutStrategy(FanoutStrategy strategy) {
        return new PostEvent(eventId, postId, idempotencyKey, authorId, caption,
                s3Key, cdnUrl, mediaType, followerCount, strategy, occurredAt);
    }
}
