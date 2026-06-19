package com.coresys.common.events.instagram;

/**
 * Instagram topic layout:
 *
 * posts.incoming.v1        → new post from user
 * posts.fanout.v1          → fanout worker distributes to follower timelines
 * follow.events.v1         → follow/unfollow social graph events
 * feed.updates.v1          → timeline cache write events
 * instagram.dlq.v1         → dead letter queue
 * instagram.recon.v1       → feed consistency discrepancies
 */
public final class InstagramTopics {
    private InstagramTopics() {}
    public static final String POSTS_INCOMING     = "instagram.posts.incoming.v1";
    public static final String POSTS_FANOUT       = "instagram.posts.fanout.v1";
    public static final String FOLLOW_EVENTS      = "instagram.follow.events.v1";
    public static final String FEED_UPDATES       = "instagram.feed.updates.v1";
    public static final String DLQ                = "instagram.dlq.v1";
    public static final String RECON              = "instagram.recon.v1";
}
