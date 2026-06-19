package com.coresys.state.instagram.consumer;

import com.coresys.common.events.instagram.InstagramTopics;
import com.coresys.common.events.instagram.PostEvent;
import com.coresys.state.instagram.service.PostStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CONSUMER GROUP: instagram-state-posts
 * Consumes instagram.posts.fanout.v1
 * Writes PostEntity to DB + fans out to follower timelines via PostStateService.
 *
 * Offset committed ONLY after DB write + Redis fan-out succeeds.
 * On failure → DefaultErrorHandler → instagram.dlq.v1
 */
@Component
@Profile("instagram")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class PostStateConsumer {

    private static final Logger log = LoggerFactory.getLogger(PostStateConsumer.class);
    private final PostStateService stateService;

    public PostStateConsumer(PostStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = InstagramTopics.POSTS_FANOUT, groupId = "instagram-state-posts")
    public void onPost(PostEvent event, Acknowledgment ack) {
        try {
            stateService.apply(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("PostState failed postId={}: {}", event.postId(), e.getMessage(), e);
            throw e;
        }
    }
}
