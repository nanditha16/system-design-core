package com.coresys.ingestion.instagram.api;

import com.coresys.common.events.instagram.FollowEvent;
import com.coresys.ingestion.instagram.publish.InstagramEventPublisher;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * POST /api/v1/instagram/follow   → follow
 * DELETE /api/v1/instagram/follow → unfollow
 *
 * Social graph is maintained in state-service (SQL + in-memory adjacency).
 * Follow events also trigger backfill: new follower's feed gets recent posts
 * from the followee (handled in FanoutRouter).
 */
@Profile("instagram")
@RestController
@RequestMapping("/api/v1/instagram/follow")
public class FollowController {

    private final InstagramEventPublisher publisher;

    public FollowController(InstagramEventPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> follow(@RequestBody FollowRequest request) {
        return publishFollow(request, true)
                .map(e -> ResponseEntity.accepted().body("followed"));
    }

    @DeleteMapping
    public Mono<ResponseEntity<String>> unfollow(@RequestBody FollowRequest request) {
        return publishFollow(request, false)
                .map(e -> ResponseEntity.accepted().body("unfollowed"));
    }

    private Mono<FollowEvent> publishFollow(FollowRequest req, boolean isFollow) {
        FollowEvent event = new FollowEvent(
                UUID.randomUUID().toString(),
                req.followerId(), req.followeeId(),
                isFollow, Instant.now());
        return publisher.publishFollow(event);
    }
}
