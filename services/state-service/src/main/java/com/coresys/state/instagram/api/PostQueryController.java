package com.coresys.state.instagram.api;

import com.coresys.state.instagram.domain.PostEntity;
import com.coresys.state.instagram.domain.PostRepository;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Profile("instagram")
@RestController
@RequestMapping("/api/v1/instagram/posts")
public class PostQueryController {

    private final PostRepository posts;
    public PostQueryController(PostRepository posts) { this.posts = posts; }

    @GetMapping("/{postId}")
    public ResponseEntity<PostEntity> getPost(@PathVariable String postId) {
        return posts.findByPostId(postId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/user/{authorId}")
    public List<PostEntity> getUserPosts(@PathVariable String authorId) {
        return posts.findByAuthorIdOrderByCreatedAtDesc(authorId);
    }
}
