package com.coresys.state.instagram.service;

import com.coresys.common.events.instagram.FeedUpdateEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Redis Timeline Cache — the heart of feed delivery.
 *
 * Data structure: Redis Sorted Set (ZSET)
 *   Key:   timeline:{userId}
 *   Score: epoch millis of post (enables chronological ordering)
 *   Value: JSON-serialized FeedUpdateEvent
 *
 * Operations:
 *   ZADD   → fan-out write (O(log N))
 *   ZREVRANGE → feed read, newest first (O(log N + M))
 *   ZREMRANGEBYRANK → trim to maxTimelineSize (O(log N + M))
 *
 * Interview talking point: "We cap the timeline at 800 entries (Instagram-style).
 * Older posts are evicted. Users who scroll past 800 posts trigger a DB read
 * (fan-out on read fallback for deep pagination)."
 *
 * Fan-out on read for mega-influencers: their posts are NOT written here.
 * Feed assembly merges this ZSET with a live query for mega-influencer posts.
 */
@Service
public class TimelineCacheService {

    private static final Logger log = LoggerFactory.getLogger(TimelineCacheService.class);
    private static final String KEY_PREFIX = "timeline:";

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final long maxTimelineSize;

    public TimelineCacheService(StringRedisTemplate redis, ObjectMapper mapper,
                                @Value("${instagram.timeline.max-size:800}") long maxSize) {
        this.redis = redis;
        this.mapper = mapper;
        this.maxTimelineSize = maxSize;
    }

    /**
     * Fan-out on write: push one post into one follower's timeline.
     * Called once per follower in the fan-out worker loop.
     */
    public void pushToTimeline(FeedUpdateEvent update) {
        try {
            String key = KEY_PREFIX + update.followerId();
            String value = mapper.writeValueAsString(update);
            double score = update.postedAt().toEpochMilli();

            redis.opsForZSet().add(key, value, score);

            // Trim to max size (keep only the N most recent entries)
            redis.opsForZSet().removeRange(key, 0, -(maxTimelineSize + 1));

            log.debug("Timeline push: user={} postId={}", update.followerId(), update.postId());
        } catch (Exception e) {
            log.error("Timeline cache write failed user={}: {}", update.followerId(), e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * Feed read: fetch N most recent posts from timeline cache.
     * Returns raw JSON strings; controller deserializes.
     */
    public List<String> getTimeline(String userId, int limit) {
        String key = KEY_PREFIX + userId;
        var entries = redis.opsForZSet().reverseRange(key, 0, limit - 1);
        return entries == null ? List.of() : List.copyOf(entries);
    }

    /** Remove a specific post from a user's timeline (on post delete). */
    public void removeFromTimeline(String userId, String postId) {
        String key = KEY_PREFIX + userId;
        redis.opsForZSet().scan(key, org.springframework.data.redis.core.ScanOptions.NONE)
                .forEachRemaining(entry -> {
                    if (entry.getValue().contains(postId)) {
                        redis.opsForZSet().remove(key, entry.getValue());
                    }
                });
    }

    public long getTimelineSize(String userId) {
        Long size = redis.opsForZSet().size(KEY_PREFIX + userId);
        return size == null ? 0 : size;
    }
}
