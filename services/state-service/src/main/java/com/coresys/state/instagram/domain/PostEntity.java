package com.coresys.state.instagram.domain;

import com.coresys.common.events.instagram.FanoutStrategy;
import com.coresys.common.events.instagram.MediaType;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Post metadata stored in PostgreSQL.
 * Raw media lives in S3; only the CDN URL is stored here.
 *
 * UNIQUE(post_id)         → exactly-once backstop
 * UNIQUE(idempotency_key) → API-level dedup backstop
 * Index on author_id + created_at → efficient per-user post listing
 */
@Entity
@Table(name = "instagram_posts", uniqueConstraints = {
        @UniqueConstraint(name = "uq_post_id",   columnNames = "post_id"),
        @UniqueConstraint(name = "uq_post_idem",  columnNames = "idempotency_key")
})
public class PostEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "post_id",         nullable = false) private String postId;
    @Column(name = "idempotency_key", nullable = false) private String idempotencyKey;
    @Column(name = "author_id",       nullable = false) private String authorId;
    @Column(nullable = false, length = 2200)            private String caption;
    @Column(name = "s3_key",          nullable = false) private String s3Key;
    @Column(name = "cdn_url",         nullable = false) private String cdnUrl;
    @Enumerated(EnumType.STRING)                        private MediaType mediaType;
    @Enumerated(EnumType.STRING)                        private FanoutStrategy fanoutStrategy;
    @Column(name = "follower_count")                    private long followerCount;
    @Column(name = "created_at",      nullable = false) private Instant createdAt;

    protected PostEntity() {}

    public PostEntity(String postId, String idempotencyKey, String authorId,
                      String caption, String s3Key, String cdnUrl,
                      MediaType mediaType, FanoutStrategy fanoutStrategy,
                      long followerCount) {
        this.postId = postId; this.idempotencyKey = idempotencyKey;
        this.authorId = authorId; this.caption = caption;
        this.s3Key = s3Key; this.cdnUrl = cdnUrl;
        this.mediaType = mediaType; this.fanoutStrategy = fanoutStrategy;
        this.followerCount = followerCount;
        this.createdAt = Instant.now();
    }

    public Long getId()                    { return id; }
    public String getPostId()              { return postId; }
    public String getAuthorId()            { return authorId; }
    public String getCaption()             { return caption; }
    public String getCdnUrl()              { return cdnUrl; }
    public String getS3Key()               { return s3Key; }
    public MediaType getMediaType()        { return mediaType; }
    public FanoutStrategy getFanoutStrategy() { return fanoutStrategy; }
    public long getFollowerCount()         { return followerCount; }
    public Instant getCreatedAt()          { return createdAt; }
}
