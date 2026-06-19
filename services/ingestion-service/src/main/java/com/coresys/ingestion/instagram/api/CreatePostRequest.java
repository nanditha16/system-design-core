package com.coresys.ingestion.instagram.api;

import com.coresys.common.events.instagram.MediaType;

public record CreatePostRequest(
        String authorId,
        String caption,
        String s3Key,       // client uploads to S3 first, sends key here
        MediaType mediaType
) {}
