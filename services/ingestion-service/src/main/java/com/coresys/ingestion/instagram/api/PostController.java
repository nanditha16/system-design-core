package com.coresys.ingestion.instagram.api;

import com.coresys.common.events.instagram.*;
import com.coresys.ingestion.feature.idempotency.IdempotencyService;
import com.coresys.ingestion.instagram.publish.InstagramEventPublisher;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * POST /api/v1/instagram/posts
 *
 * Client flow (production):
 *   1. Client requests a pre-signed S3 URL from a separate Media Upload Service
 *   2. Client uploads media directly to S3 (bypass our servers)
 *   3. Client sends s3Key here to register the post
 *
 * CDN seam: s3Key is rewritten to cdnUrl in InstagramEventPublisher.
 * Rate limiting: enforced at API Gateway (not shown here).
 * Exactly-once: Idempotency-Key prevents duplicate posts on client retry.
 */
@Profile("instagram")
@RestController
@RequestMapping("/api/v1/instagram/posts")
public class PostController {

    private final IdempotencyService idempotency;
    private final InstagramEventPublisher publisher;

    public PostController(IdempotencyService idempotency, InstagramEventPublisher publisher) {
        this.idempotency = idempotency;
        this.publisher = publisher;
    }

    @PostMapping
    public Mono<ResponseEntity<PostResponse>> createPost(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody CreatePostRequest request) {

        return idempotency.reserve(idempotencyKey)
                .flatMap(reserved -> reserved
                        ? publishPost(idempotencyKey, request)
                        : Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(new PostResponse(null, "DUPLICATE",
                                        "Post already submitted"))));
    }

    private Mono<ResponseEntity<PostResponse>> publishPost(String key, CreatePostRequest req) {
        String postId = UUID.randomUUID().toString();
        // CDN URL rewrite seam (production: CloudFront distribution domain)
        String cdnUrl = "https://cdn.example.com/posts/" + postId + "/" +
                req.s3Key().replaceAll(".*/", "");

        PostEvent event = new PostEvent(
                UUID.randomUUID().toString(),
                postId, key,
                req.authorId(), req.caption(),
                req.s3Key(), cdnUrl,
                req.mediaType(),
                0L,                         // followerCount fetched in processing
                FanoutStrategy.WRITE,        // default; router may override
                Instant.now());

        return publisher.publishPost(event)
                .map(e -> ResponseEntity.accepted()
                        .body(new PostResponse(e.postId(), "PENDING", "accepted")));
    }
}
