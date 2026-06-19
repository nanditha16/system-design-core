package com.coresys.common.events.instagram;

import java.time.Instant;

public record FollowEvent(
        String eventId,
        String followerId,
        String followeeId,
        boolean isFollow,   // true=follow, false=unfollow
        Instant occurredAt
) {}
