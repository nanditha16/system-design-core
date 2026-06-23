package com.coresys.ingestion.ebay.api;

import com.coresys.common.events.ebay.*;
import com.coresys.ingestion.ebay.publish.EbayEventPublisher;
import com.coresys.ingestion.feature.idempotency.IdempotencyService;
import org.springframework.context.annotation.Profile;
import org.springframework.graphql.data.method.annotation.Argument;
import org.springframework.graphql.data.method.annotation.MutationMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Spring GraphQL @Controller — handles GraphQL mutations.
 *
 * WHY GraphQL inside ingestion-service (not a new service):
 *   Same pattern as REST controllers (PostController, PaymentController) — they all
 *   live in ingestion-service and publish to Kafka. GraphQL is just a different
 *   transport layer; it doesn't require a new service boundary.
 *
 * @MutationMapping → maps to type Mutation in schema.graphqls
 *
 * IDEMPOTENCY:
 *   createListing carries idempotencyKey (X-Idempotency-Key header / GraphQL variable).
 *   Redis SETNX check before publishing — prevents double-create on client retry.
 *
 * NOTE: ingestion-service is WebFlux (reactive). Spring GraphQL works with both
 *   WebMVC and WebFlux; here we use blocking .block() on the reactive Mono only
 *   inside the async subscription thread, not on the Netty event loop.
 */
@Profile("ebay")
@Controller
public class EbayMutationController {

    private final IdempotencyService idempotency;
    private final EbayEventPublisher publisher;

    public EbayMutationController(IdempotencyService idempotency, EbayEventPublisher publisher) {
        this.idempotency = idempotency;
        this.publisher = publisher;
    }

    /**
     * onboardSeller mutation — no idempotency needed (email uniqueness enforced in DB).
     */
    @MutationMapping
    public Mono<String> onboardSeller(@Argument SellerInput input) {
        String sellerId = UUID.randomUUID().toString();
        SellerEvent event = new SellerEvent(
                UUID.randomUUID().toString(), sellerId,
                input.businessName(), input.email(), input.countryCode(),
                "ONBOARDED", Instant.now());
        return publisher.publishSellerEvent(event)
                .map(e -> sellerId);
    }

    /**
     * createListing mutation — idempotency-keyed to prevent double-create.
     * Returns listingId immediately (202 semantics via GraphQL).
     * Listing starts as DRAFT; enrichment pipeline activates it.
     */
    @MutationMapping
    public Mono<String> createListing(@Argument CreateListingInput input,
                                       @Argument String idempotencyKey) {
        return idempotency.reserve(idempotencyKey)
                .flatMap(reserved -> {
                    if (!reserved) return Mono.just("DUPLICATE:" + idempotencyKey);
                    String listingId = UUID.randomUUID().toString();
                    ListingEvent event = new ListingEvent(
                            UUID.randomUUID().toString(), listingId, idempotencyKey,
                            input.sellerId(), input.productId(),
                            input.price(), input.currency(), input.region(),
                            input.initialQty(),
                            false, false, 0.0, false, null,
                            ListingStatus.DRAFT, Instant.now());
                    return publisher.publishListingEvent(event).map(e -> listingId);
                });
    }

    /**
     * adjustInventory mutation — ADD / RESERVE / RELEASE / SELL.
     * Uses optimistic locking in state-service to prevent overselling.
     */
    @MutationMapping
    public Mono<String> adjustInventory(@Argument InventoryAdjustmentInput input) {
        InventoryEvent event = new InventoryEvent(
                UUID.randomUUID().toString(),
                input.listingId(),
                InventoryEventType.valueOf(input.eventType()),
                input.quantity(),
                input.orderId(),
                Instant.now());
        return publisher.publishInventoryEvent(event)
                .map(e -> "ACCEPTED");
    }
}
