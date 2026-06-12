package com.coresys.ingestion.api;

import com.coresys.common.dto.TransactionRequest;
import com.coresys.common.dto.TransactionResponse;
import com.coresys.common.events.TransactionEvent;
import com.coresys.common.events.TransactionStatus;
import com.coresys.ingestion.feature.idempotency.IdempotencyService;
import com.coresys.ingestion.feature.publish.EventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Reactive entrypoint. Flow:
 * 1. Reserve idempotency key (Redis SETNX) -> reject duplicates with 409
 * 2. Publish PENDING event (Kafka or sync REST, depending on feature flag)
 * 3. Return 202 Accepted (async contract: client polls or receives webhook)
 */
@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final IdempotencyService idempotency;
    private final EventPublisher publisher;

    public TransactionController(IdempotencyService idempotency, EventPublisher publisher) {
        this.idempotency = idempotency;
        this.publisher = publisher;
    }

    @PostMapping
    public Mono<ResponseEntity<TransactionResponse>> submit(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody TransactionRequest request) {

        return idempotency.reserve(idempotencyKey)
                .flatMap(reserved -> reserved
                        ? publishNew(idempotencyKey, request)
                        : Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(new TransactionResponse(null, "DUPLICATE",
                                        "Idempotency-Key already processed"))));
    }

    private Mono<ResponseEntity<TransactionResponse>> publishNew(String key, TransactionRequest req) {
        TransactionEvent event = new TransactionEvent(
                UUID.randomUUID().toString(),          // eventId (consumer dedup)
                UUID.randomUUID().toString(),          // transactionId (business key)
                key, req.amount(), req.currency(), req.region(), req.type(),
                TransactionStatus.PENDING, Instant.now());

        return publisher.publish(event)
                .map(e -> ResponseEntity.accepted()
                        .body(new TransactionResponse(e.transactionId(), e.status().name(), "accepted")));
    }
}
