package com.coresys.state.instagram.api;

import com.coresys.state.instagram.domain.UserFollowRepository;
import com.coresys.state.instagram.service.TimelineCacheService;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Profile("instagram")
@RestController
@RequestMapping("/api/v1/instagram/users")
public class UserController {

    private final UserFollowRepository follows;
    private final TimelineCacheService timelineCache;

    public UserController(UserFollowRepository follows, TimelineCacheService timelineCache) {
        this.follows = follows;
        this.timelineCache = timelineCache;
    }

    /** Used by FanoutRouter to decide write vs read strategy. */
    @GetMapping("/{userId}/follower-count")
    public long getFollowerCount(@PathVariable String userId) {
        return follows.countByFolloweeId(userId);
    }

    @GetMapping("/{userId}/stats")
    public Map<String, Object> getUserStats(@PathVariable String userId) {
        return Map.of(
                "followersCount", follows.countByFolloweeId(userId),
                "followingCount", follows.findByFollowerId(userId).size(),
                "timelineSize",   timelineCache.getTimelineSize(userId)
        );
    }
}
