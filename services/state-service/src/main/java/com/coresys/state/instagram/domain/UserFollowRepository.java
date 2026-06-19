package com.coresys.state.instagram.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import java.util.List;

public interface UserFollowRepository extends JpaRepository<UserFollowEntity, Long> {
    List<UserFollowEntity> findByFolloweeId(String followeeId);   // who follows this author?
    List<UserFollowEntity> findByFollowerId(String followerId);   // who does this user follow?
    long countByFolloweeId(String followeeId);                    // follower count
    void deleteByFollowerIdAndFolloweeId(String followerId, String followeeId);
    boolean existsByFollowerIdAndFolloweeId(String followerId, String followeeId);

    @Query("SELECT f.followeeId FROM UserFollowEntity f WHERE f.followerId = :followerId")
    List<String> findFolloweeIdsByFollowerId(String followerId);
}
