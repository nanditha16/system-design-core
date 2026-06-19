package com.coresys.state.instagram.api;

import com.coresys.common.events.instagram.FeedUpdateEvent;
import com.coresys.common.events.instagram.FanoutStrategy;
import com.coresys.state.instagram.domain.PostEntity;
import com.coresys.state.instagram.domain.PostRepository;
import com.coresys.state.instagram.domain.UserFollowRepository;
import com.coresys.state.instagram.service.TimelineCacheService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * GET /api/v1/instagram/feed/{userId}?limit=20
 *
 * Feed assembly strategy:
 *
 * 1. FAST PATH (normal users):
 *    Read Redis ZSET (timeline:{userId}) — O(log N + M), sub-millisecond.
 *    Returns pre-computed fan-out results.
 *
 * 2. MEGA-INFLUENCER MERGE:
 *    Detect followees with FanoutStrategy.READ in follow list.
 *    Fetch their recent posts from DB directly (not in cache).
 *    Merge + sort with Redis results.
 *
 * Interview talking point:
 * "This is the hybrid feed pattern. We read 95% from Redis O(1) and supplement
 *  with at most a handful of DB reads for mega-influencer followees. The merge
 *  cost is O(K log K) where K = limit (e.g. 20), which is constant."
 */
@Profile("instagram")
@RestController
@RequestMapping("/api/v1/instagram/feed")
public class FeedController {

    private final TimelineCacheService timelineCache;
    private final PostRepository posts;
    private final UserFollowRepository follows;
    private final ObjectMapper mapper;

    public FeedController(TimelineCacheService timelineCache, PostRepository posts,
                          UserFollowRepository follows, ObjectMapper mapper) {
        this.timelineCache = timelineCache;
        this.posts = posts;
        this.follows = follows;
        this.mapper = mapper;
    }

    @GetMapping("/{userId}")
    public List<FeedItem> getFeed(@PathVariable String userId,
                                  @RequestParam(defaultValue = "20") int limit) {
        // 1. Read pre-computed timeline from Redis
        List<String> cached = timelineCache.getTimeline(userId, limit);
        List<FeedItem> items = new ArrayList<>();

        cached.forEach(json -> {
            try {
                FeedUpdateEvent e = mapper.readValue(json, FeedUpdateEvent.class);
                items.add(new FeedItem(e.postId(), e.authorId(), e.cdnUrl(),
                        e.mediaType().name(), e.caption(), e.postedAt().toString()));
            } catch (Exception ignored) {}
        });

        // 2. Merge mega-influencer posts (fan-out on read)
        List<String> followeeIds = follows.findFolloweeIdsByFollowerId(userId);
        followeeIds.forEach(followeeId -> {
            List<PostEntity> megaPosts = posts.findTop20ByAuthorIdOrderByCreatedAtDesc(followeeId)
                    .stream()
                    .filter(p -> p.getFanoutStrategy() == FanoutStrategy.READ)
                    .toList();
            megaPosts.forEach(p -> items.add(new FeedItem(p.getPostId(), p.getAuthorId(),
                    p.getCdnUrl(), p.getMediaType().name(), p.getCaption(),
                    p.getCreatedAt().toString())));
        });

        // 3. Sort merged list by createdAt descending, cap at limit
        items.sort(Comparator.comparing(FeedItem::postedAt).reversed());
        return items.stream().limit(limit).toList();
    }

    public record FeedItem(String postId, String authorId, String cdnUrl,
                           String mediaType, String caption, String postedAt) {}
}
