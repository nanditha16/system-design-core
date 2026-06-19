package com.coresys.state.instagram.consumer;

import com.coresys.common.events.instagram.FollowEvent;
import com.coresys.common.events.instagram.InstagramTopics;
import com.coresys.state.instagram.service.FollowStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CONSUMER GROUP: instagram-state-follows
 * Consumes instagram.feed.updates.v1 (follow/unfollow events forwarded by FollowEventConsumer)
 * Updates social graph in DB + backfills timeline on new follow.
 */
@Component
@Profile("instagram")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class FollowStateConsumer {

    private static final Logger log = LoggerFactory.getLogger(FollowStateConsumer.class);
    private final FollowStateService followStateService;

    public FollowStateConsumer(FollowStateService followStateService) {
        this.followStateService = followStateService;
    }

    @KafkaListener(topics = InstagramTopics.FEED_UPDATES, groupId = "instagram-state-follows")
    public void onFollowEvent(FollowEvent event, Acknowledgment ack) {
        try {
            followStateService.apply(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("FollowState failed followerId={}: {}", event.followerId(), e.getMessage(), e);
            throw e;
        }
    }
}
