package com.coresys.processing.instagram.consumer;

import com.coresys.common.events.instagram.*;
import com.coresys.processing.instagram.routing.FanoutRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CONSUMER GROUP: instagram-fanout
 * Consumes instagram.posts.incoming.v1
 * Determines fanout strategy via FanoutRouter
 * Publishes to instagram.posts.fanout.v1 (picked up by feed-update workers in state-service)
 *
 * Manual ack: offset committed only after successful downstream publish.
 * Failure → DefaultErrorHandler (exponential backoff) → instagram.dlq.v1
 */
@Profile("instagram")
@Component
public class PostFanoutConsumer {

    private static final Logger log = LoggerFactory.getLogger(PostFanoutConsumer.class);

    private final FanoutRouter router;
    private final KafkaTemplate<String, Object> kafka;

    public PostFanoutConsumer(FanoutRouter router, KafkaTemplate<String, Object> kafka) {
        this.router = router;
        this.kafka = kafka;
    }

    @KafkaListener(topics = InstagramTopics.POSTS_INCOMING, groupId = "instagram-fanout")
    public void onPost(PostEvent event, Acknowledgment ack) {
        try {
            PostEvent routed = router.route(event).block();
            kafka.send(InstagramTopics.POSTS_FANOUT, routed.postId(), routed).join();
            ack.acknowledge();
            log.info("Post fanout: postId={} strategy={} followers={}",
                    routed.postId(), routed.fanoutStrategy(), routed.followerCount());
        } catch (Exception e) {
            log.error("Fanout failed postId={}: {}", event.postId(), e.getMessage(), e);
            throw e;
        }
    }
}
