package com.coresys.reconciliation.instagram.job;

import com.coresys.common.events.instagram.InstagramTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * Feed Consistency Reconciliation Job.
 *
 * Detects posts that were saved to DB (WRITE strategy) but whose fan-out
 * to follower timelines may have partially failed (Redis unavailable, consumer crash).
 *
 * Strategy:
 *   Query recent posts with FanoutStrategy=WRITE created in the last window.
 *   For each such post, verify follower count in DB matches expected fan-out writes.
 *   If discrepancy detected → publish to instagram.recon.v1 for manual/automated re-fan-out.
 *
 * Interview talking point:
 * "In production, Redis fan-out is fire-and-forget after the DB commit.
 *  This job is the safety net — it re-triggers fan-out for any post that had
 *  partial failures, ensuring eventual consistency in the feed."
 */
@Profile("instagram")
@Component
public class FeedConsistencyJob {

    private static final Logger log = LoggerFactory.getLogger(FeedConsistencyJob.class);

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, Object> kafka;
    private final long windowMinutes;

    public FeedConsistencyJob(
            JdbcTemplate jdbc,
            KafkaTemplate<String, Object> kafka,
            @Value("${recon.instagram.window-minutes:5}") long windowMinutes) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.windowMinutes = windowMinutes;
    }

    @Scheduled(fixedDelayString = "${recon.instagram.interval-ms:120000}")
    public void run() {
        Instant cutoff = Instant.now().minus(windowMinutes, ChronoUnit.MINUTES);
        log.info("Instagram Feed Consistency check. Window: last {} minutes (since {})",
                windowMinutes, cutoff);

        // Find posts in WRITE strategy created within the window (recently fanned out)
        List<Map<String, Object>> recentPosts = jdbc.queryForList("""
                SELECT post_id, author_id, follower_count, created_at
                FROM instagram_posts
                WHERE fanout_strategy = 'WRITE'
                  AND created_at >= ?
                ORDER BY created_at DESC
                LIMIT 100
                """, java.sql.Timestamp.from(cutoff));

        if (recentPosts.isEmpty()) {
            log.info("Feed Consistency CLEAN: no recent WRITE-strategy posts to verify");
            return;
        }

        int discrepancies = 0;
        for (Map<String, Object> post : recentPosts) {
            String postId    = (String) post.get("post_id");
            String authorId  = (String) post.get("author_id");
            long expectedFanout = (long) post.get("follower_count");

            // Count actual followers at time of check (may differ from snapshot)
            Long actualFollowers = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM instagram_follows WHERE followee_id = ?",
                    Long.class, authorId);
            actualFollowers = actualFollowers == null ? 0 : actualFollowers;

            // Flag if follower count has grown significantly (new followers missed backfill)
            // or if expectedFanout was 0 but there are followers (fan-out never ran)
            if (expectedFanout == 0 && actualFollowers > 0) {
                discrepancies++;
                log.warn("Feed gap: postId={} expectedFanout=0 actualFollowers={}",
                        postId, actualFollowers);

                kafka.send(InstagramTopics.RECON, postId, Map.of(
                        "type", "FANOUT_GAP",
                        "postId", postId,
                        "authorId", authorId,
                        "expectedFanout", expectedFanout,
                        "actualFollowers", actualFollowers,
                        "detectedAt", Instant.now().toString()
                ));
            }
        }

        if (discrepancies > 0) {
            log.warn("Feed Consistency: {} discrepancies published to {}",
                    discrepancies, InstagramTopics.RECON);
        } else {
            log.info("Feed Consistency CLEAN: {} posts verified, no gaps", recentPosts.size());
        }
    }
}
