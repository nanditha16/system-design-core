package com.coresys.state.instagram.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;

public interface PostRepository extends JpaRepository<PostEntity, Long> {
    Optional<PostEntity> findByPostId(String postId);
    List<PostEntity> findByAuthorIdOrderByCreatedAtDesc(String authorId);
    // Top N recent posts for a given author (used in follow backfill + fan-out on read)
    List<PostEntity> findTop20ByAuthorIdOrderByCreatedAtDesc(String authorId);
}
