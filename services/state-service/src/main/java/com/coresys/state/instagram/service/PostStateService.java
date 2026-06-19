package com.coresys.state.instagram.service;

import com.coresys.common.events.instagram.*;
import com.coresys.state.domain.ProcessedEventEntity;
import com.coresys.state.domain.ProcessedEventRepository;
import com.coresys.state.instagram.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Single-writer for post state + fan-out on write.
 *
 * One ACID transaction wraps:
 *   1. eventId dedup (processed_events)
 *   2. PostEntity insert
 *   3. Fan-out on WRITE: push to each follower's timeline (Redis, outside transaction)
 *      Fan-out on READ:  skip Redis writes (mega-influencer path)
 *
 * Interview talking point: "The DB write and Redis fan-out are NOT in the same
 * transaction. If Redis fails after DB commit, the post is saved but some timelines
 * miss it. The reconciliation job detects this and re-triggers fan-out."
 */
@Service
public class PostStateService {

    private static final Logger log = LoggerFactory.getLogger(PostStateService.class);

    private final PostRepository posts;
    private final UserFollowRepository follows;
    private final ProcessedEventRepository processedEvents;
    private final TimelineCacheService timelineCache;

    public PostStateService(PostRepository posts, UserFollowRepository follows,
                            ProcessedEventRepository processedEvents,
                            TimelineCacheService timelineCache) {
        this.posts = posts;
        this.follows = follows;
        this.processedEvents = processedEvents;
        this.timelineCache = timelineCache;
    }

    @Transactional
    public void apply(PostEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate post event {} skipped", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEventEntity(event.eventId()));

        try {
            PostEntity post = posts.save(new PostEntity(
                    event.postId(), event.idempotencyKey(), event.authorId(),
                    event.caption(), event.s3Key(), event.cdnUrl(),
                    event.mediaType(), event.fanoutStrategy(), event.followerCount()));

            log.info("Post saved: postId={} author={} strategy={}",
                    post.getPostId(), post.getAuthorId(), post.getFanoutStrategy());

        } catch (DataIntegrityViolationException e) {
            log.warn("Idempotent skip post={}: {}", event.postId(), e.getMessage());
            return;
        }

        // Fan-out on WRITE: push to all follower timelines
        if (event.fanoutStrategy() == FanoutStrategy.WRITE) {
            fanoutToFollowers(event);
        } else {
            log.info("Mega-influencer postId={} followers={}: skipping fan-out (read-time assembly)",
                    event.postId(), event.followerCount());
        }
    }

    private void fanoutToFollowers(PostEvent event) {
        List<UserFollowEntity> followers = follows.findByFolloweeId(event.authorId());
        log.info("Fan-out post={} to {} followers", event.postId(), followers.size());

        followers.forEach(follow -> {
            FeedUpdateEvent update = new FeedUpdateEvent(
                    java.util.UUID.randomUUID().toString(),
                    follow.getFollowerId(),
                    event.postId(), event.authorId(),
                    event.cdnUrl(), event.mediaType(),
                    event.caption(), event.occurredAt());
            timelineCache.pushToTimeline(update);
        });
    }

    public long getFollowerCount(String authorId) {
        return follows.countByFolloweeId(authorId);
    }
}
