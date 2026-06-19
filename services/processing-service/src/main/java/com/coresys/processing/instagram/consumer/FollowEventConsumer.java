package com.coresys.processing.instagram.consumer;

import com.coresys.common.events.instagram.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CONSUMER GROUP: instagram-follow-processor
 * Consumes instagram.follow.events.v1
 * Forwards to instagram.feed.updates.v1 for state-service to persist social graph.
 *
 * On new follow: triggers backfill of recent N posts from followee into follower's timeline.
 * This backfill is handled in state-service FollowStateService.
 */
@Profile("instagram")
@Component
public class FollowEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(FollowEventConsumer.class);
    private final KafkaTemplate<String, Object> kafka;

    public FollowEventConsumer(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @KafkaListener(topics = InstagramTopics.FOLLOW_EVENTS, groupId = "instagram-follow-processor")
    public void onFollow(FollowEvent event, Acknowledgment ack) {
        try {
            kafka.send(InstagramTopics.FEED_UPDATES, event.followerId(), event).join();
            ack.acknowledge();
            log.info("Follow event: {} {} {}",
                    event.followerId(),
                    event.isFollow() ? "->" : "X",
                    event.followeeId());
        } catch (Exception e) {
            log.error("Follow processing failed: {}", e.getMessage(), e);
            throw e;
        }
    }
}
