package com.coresys.state.instagram.service;

import com.coresys.common.events.instagram.*;
import com.coresys.state.domain.ProcessedEventEntity;
import com.coresys.state.domain.ProcessedEventRepository;
import com.coresys.state.instagram.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Manages social graph (follow/unfollow) and feed backfill.
 *
 * On FOLLOW:
 *   1. Insert edge: follower → followee
 *   2. Backfill: push followee's last 20 posts into follower's timeline
 *      (so new followers see content immediately, not an empty feed)
 *
 * On UNFOLLOW:
 *   1. Delete edge
 *   2. Remove followee's posts from follower's timeline (optional cleanup)
 */
@Service
public class FollowStateService {

    private static final Logger log = LoggerFactory.getLogger(FollowStateService.class);

    private final UserFollowRepository follows;
    private final PostRepository posts;
    private final ProcessedEventRepository processedEvents;
    private final TimelineCacheService timelineCache;

    public FollowStateService(UserFollowRepository follows, PostRepository posts,
                              ProcessedEventRepository processedEvents,
                              TimelineCacheService timelineCache) {
        this.follows = follows;
        this.posts = posts;
        this.processedEvents = processedEvents;
        this.timelineCache = timelineCache;
    }

    @Transactional
    public void apply(FollowEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate follow event {} skipped", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEventEntity(event.eventId()));

        if (event.isFollow()) {
            handleFollow(event);
        } else {
            handleUnfollow(event);
        }
    }

    private void handleFollow(FollowEvent event) {
        if (!follows.existsByFollowerIdAndFolloweeId(event.followerId(), event.followeeId())) {
            follows.save(new UserFollowEntity(event.followerId(), event.followeeId()));
            log.info("Follow: {} -> {}", event.followerId(), event.followeeId());
            backfillTimeline(event.followerId(), event.followeeId());
        }
    }

    private void handleUnfollow(FollowEvent event) {
        follows.deleteByFollowerIdAndFolloweeId(event.followerId(), event.followeeId());
        log.info("Unfollow: {} X {}", event.followerId(), event.followeeId());
    }

    /** Push followee's recent posts into new follower's timeline. */
    private void backfillTimeline(String followerId, String followeeId) {
        List<PostEntity> recentPosts = posts.findTop20ByAuthorIdOrderByCreatedAtDesc(followeeId);
        recentPosts.forEach(post -> {
            FeedUpdateEvent update = new FeedUpdateEvent(
                    java.util.UUID.randomUUID().toString(),
                    followerId, post.getPostId(), post.getAuthorId(),
                    post.getCdnUrl(), post.getMediaType(),
                    post.getCaption(), post.getCreatedAt());
            timelineCache.pushToTimeline(update);
        });
        log.info("Backfilled {} posts for {} from {}", recentPosts.size(), followerId, followeeId);
    }
}
