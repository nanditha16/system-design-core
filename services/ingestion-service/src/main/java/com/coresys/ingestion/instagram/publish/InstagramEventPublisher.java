package com.coresys.ingestion.instagram.publish;

import com.coresys.common.events.instagram.*;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Profile("instagram")
@Service
public class InstagramEventPublisher {

    private final KafkaTemplate<String, Object> kafka;

    public InstagramEventPublisher(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    /** Key = authorId → ordered per author, enables per-user compaction */
    public Mono<PostEvent> publishPost(PostEvent event) {
        return Mono.fromFuture(
                kafka.send(InstagramTopics.POSTS_INCOMING, event.authorId(), event)
                     .thenApply(r -> event));
    }

    public Mono<FollowEvent> publishFollow(FollowEvent event) {
        return Mono.fromFuture(
                kafka.send(InstagramTopics.FOLLOW_EVENTS, event.followerId(), event)
                     .thenApply(r -> event));
    }
}
