package com.coresys.processing.instagram.routing;

import com.coresys.common.events.instagram.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * STRUCTURAL FLAG: Fan-out strategy selection.
 *
 * WRITE path (default, <megaInfluencerThreshold followers):
 *   Fetch follower list → emit one FeedUpdateEvent per follower → Redis fan-out workers update timelines.
 *   Trade-off: high write amplification (1 post × 1M followers = 1M writes), but O(1) feed reads.
 *
 * READ path (mega-influencers, >megaInfluencerThreshold followers):
 *   Skip fan-out entirely. Feed assembled at read time by merging:
 *     - Redis timelines of normal followees
 *     - Direct DB/cache query for mega-influencer posts
 *   Trade-off: slightly slower feed load, but prevents thundering herd on write.
 *
 * Interview talking point: "Twitter/Instagram use a hybrid: fan-out on write for normal users,
 * fan-out on read for celebrities. The threshold is typically ~500K-1M followers."
 */
@Profile("instagram")
@Component
public class FanoutRouter {

    private static final Logger log = LoggerFactory.getLogger(FanoutRouter.class);

    private final long megaInfluencerThreshold;
    private final WebClient stateClient;

    public FanoutRouter(
            @Value("${instagram.mega-influencer-threshold:1000000}") long threshold,
            @Value("${downstream.state-url:http://localhost:8083}") String stateUrl) {
        this.megaInfluencerThreshold = threshold;
        this.stateClient = WebClient.create(stateUrl);
    }

    /**
     * Determine fanout strategy and enrich post event with real follower count.
     * In production: follower count fetched from a pre-computed counter in Redis/DB.
     */
    public Mono<PostEvent> route(PostEvent event) {
        return fetchFollowerCount(event.authorId())
                .map(count -> {
                    FanoutStrategy strategy = count >= megaInfluencerThreshold
                            ? FanoutStrategy.READ
                            : FanoutStrategy.WRITE;

                    log.info("Post={} author={} followers={} strategy={}",
                            event.postId(), event.authorId(), count, strategy);

                    // Rebuild event with real count + strategy
                    return new PostEvent(event.eventId(), event.postId(),
                            event.idempotencyKey(), event.authorId(), event.caption(),
                            event.s3Key(), event.cdnUrl(), event.mediaType(),
                            count, strategy, event.occurredAt());
                });
    }

    /** Fetch follower count from state-service. Stub returns 500 for demo. */
    private Mono<Long> fetchFollowerCount(String authorId) {
        return stateClient.get()
                .uri("/api/v1/instagram/users/{authorId}/follower-count", authorId)
                .retrieve()
                .bodyToMono(Long.class)
                .onErrorReturn(500L); // fallback: treat as normal user
    }
}
