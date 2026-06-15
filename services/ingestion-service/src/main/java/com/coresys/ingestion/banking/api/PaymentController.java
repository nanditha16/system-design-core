package com.coresys.ingestion.banking.api;

import com.coresys.common.events.banking.*;
import com.coresys.ingestion.feature.idempotency.IdempotencyService;
import com.coresys.ingestion.feature.publish.BankingEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Payment ingestion entrypoint.
 * Returns 202 Accepted immediately — payment processing is async.
 * Client polls GET /api/v1/banking/payments/{paymentId} for final status.
 *
 * Security seam: in production, this sits behind the API Gateway which
 * enforces JWT auth, rate limiting (Redis token bucket), and TLS termination.
 */
@RestController
@RequestMapping("/api/v1/banking/payments")
public class PaymentController {

    private final IdempotencyService idempotency;
    private final BankingEventPublisher publisher;

    public PaymentController(IdempotencyService idempotency, BankingEventPublisher publisher) {
        this.idempotency = idempotency;
        this.publisher = publisher;
    }

    @PostMapping
    public Mono<ResponseEntity<PaymentResponse>> submit(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody PaymentRequest request) {

        return idempotency.reserve(idempotencyKey)
                .flatMap(reserved -> reserved
                        ? publishPayment(idempotencyKey, request)
                        : Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(new PaymentResponse(null, "DUPLICATE",
                                        "Idempotency-Key already processed"))));
    }

    private Mono<ResponseEntity<PaymentResponse>> publishPayment(
            String key, PaymentRequest req) {

        PaymentEvent event = new PaymentEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                key,
                req.userId(),
                req.amount(),
                req.currency(),
                req.type(),
                req.region(),
                PaymentStatus.PENDING,
                null,
                Instant.now());

        return publisher.publish(event)
                .map(e -> ResponseEntity.accepted()
                        .body(new PaymentResponse(e.paymentId(), e.status().name(), "accepted")));
    }
}
