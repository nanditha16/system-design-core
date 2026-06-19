package com.coresys.common.events.instagram;

import java.time.Instant;

/**
 * Written to Redis timeline cache per follower.
 * One PostEvent → N FeedUpdateEvents (one per follower).
 */
public record FeedUpdateEvent(
        String eventId,
        String followerId,
        String postId,
        String authorId,
        String cdnUrl,
        MediaType mediaType,
        String caption,
        Instant postedAt
) {}
