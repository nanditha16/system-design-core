package com.coresys.ingestion.brokerage.api;

import com.coresys.common.events.brokerage.*;
import com.coresys.ingestion.feature.idempotency.IdempotencyService;
import com.coresys.ingestion.feature.publish.BrokerageEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Brokerage Order API.
 * POST /api/v1/orders with Idempotency-Key header.
 * Reuses the same Redis idempotency and Kafka publisher abstraction as the generic flow.
 */
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final IdempotencyService idempotency;
    private final BrokerageEventPublisher publisher;

    public OrderController(IdempotencyService idempotency, BrokerageEventPublisher publisher) {
        this.idempotency = idempotency;
        this.publisher = publisher;
    }

    @PostMapping
    public Mono<ResponseEntity<OrderResponse>> placeOrder(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody OrderRequest request) {

        return idempotency.reserve(idempotencyKey)
                .flatMap(reserved -> reserved
                        ? publishOrder(idempotencyKey, request)
                        : Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(new OrderResponse(null, "DUPLICATE",
                                        "Idempotency-Key already processed"))));
    }

    private Mono<ResponseEntity<OrderResponse>> publishOrder(String key, OrderRequest req) {
        OrderEvent event = new OrderEvent(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                req.userId(),
                key,
                req.type(),
                req.symbol(),
                req.quantity(),
                req.price(),
                OrderStatus.PENDING,
                Instant.now());

        return publisher.publish(event)
                .map(e -> ResponseEntity.accepted()
                        .body(new OrderResponse(e.orderId(), e.status().name(), "accepted")));
    }
}
