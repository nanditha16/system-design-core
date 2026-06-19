package com.coresys.state.instagram.domain;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * Social graph edge: follower → followee.
 * Used for:
 *   1. Fan-out on write: fetch all followers of an author when post arrives
 *   2. Feed assembly (fan-out on read): fetch all followees of a reader
 *   3. Backfill: when A follows B, populate A's feed with B's recent posts
 *
 * Production note: for massive graphs (billions of edges), this moves to
 * a dedicated Graph DB (e.g., Neptune) or a sharded social graph service.
 * Here we keep it in Postgres for local demo simplicity.
 */
@Entity
@Table(name = "instagram_follows", uniqueConstraints = {
        @UniqueConstraint(name = "uq_follow_edge",
                columnNames = {"follower_id", "followee_id"})
})
public class UserFollowEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "follower_id", nullable = false) private String followerId;
    @Column(name = "followee_id", nullable = false) private String followeeId;
    @Column(name = "created_at",  nullable = false) private Instant createdAt;

    protected UserFollowEntity() {}

    public UserFollowEntity(String followerId, String followeeId) {
        this.followerId = followerId;
        this.followeeId = followeeId;
        this.createdAt = Instant.now();
    }

    public String getFollowerId() { return followerId; }
    public String getFolloweeId() { return followeeId; }
    public Instant getCreatedAt() { return createdAt; }
}
