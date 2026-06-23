package com.coresys.processing.ebay.enrichment;

import com.coresys.common.events.ebay.ListingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * MULTI-THREADED ENRICHMENT — the key eBay interview pattern.
 *
 * Problem: creating a listing involves 4 independent validation/enrichment tasks:
 *   1. Category validation  (~50ms  — checks category tree)
 *   2. Tax calculation      (~80ms  — region-specific rates)
 *   3. Compliance check     (~60ms  — prohibited items, export restrictions)
 *   4. Image processing     (~120ms — resize, CDN upload stub)
 *
 * Sequential: 50+80+60+120 = 310ms (unacceptable for <200ms SLA)
 * Parallel:   max(50,80,60,120) = 120ms ✅
 *
 * Pattern: CompletableFuture.allOf() — all 4 run concurrently.
 * Dedicated thread pool (not ForkJoinPool.commonPool):
 *   - Enrichment is I/O bound (external calls in production)
 *   - ForkJoinPool is CPU-bound work stealing; I/O blocks its threads
 *   - Dedicated pool = controlled backpressure, no starvation of other tasks
 *
 * On ALL pass → republish as enriched=true → InventoryStateConsumer activates listing
 * On ANY fail → event goes to DLQ → listing stays DRAFT → seller gets notification
 */
@Service
public class ListingEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(ListingEnrichmentService.class);

    private final Executor enrichmentPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 2,
            r -> { Thread t = new Thread(r, "enrichment"); t.setDaemon(true); return t; }
    );

    public CompletableFuture<EnrichmentResult> enrich(ListingEvent event) {
        log.info("Enrichment START listingId={} region={} — running 4 tasks in parallel",
                event.listingId(), event.region());

        CompletableFuture<Boolean> categoryFuture =
                CompletableFuture.supplyAsync(() -> validateCategory(event), enrichmentPool);

        CompletableFuture<Double> taxFuture =
                CompletableFuture.supplyAsync(() -> calculateTax(event), enrichmentPool);

        CompletableFuture<Boolean> complianceFuture =
                CompletableFuture.supplyAsync(() -> checkCompliance(event), enrichmentPool);

        CompletableFuture<String> imageFuture =
                CompletableFuture.supplyAsync(() -> processImages(event), enrichmentPool);

        return CompletableFuture.allOf(categoryFuture, taxFuture, complianceFuture, imageFuture)
                .thenApply(v -> {
                    boolean cat     = categoryFuture.join();
                    double  tax     = taxFuture.join();
                    boolean comply  = complianceFuture.join();
                    String  imgUrl  = imageFuture.join();

                    log.info("Enrichment DONE listingId={} tax={}% compliant={}",
                            event.listingId(), String.format("%.0f", tax * 100), comply);
                    return new EnrichmentResult(cat, tax, comply, imgUrl);
                });
    }

    private boolean validateCategory(ListingEvent e) {
        sleep(50, "category-validation", e.listingId());
        return e.productId() != null;
    }

    private double calculateTax(ListingEvent e) {
        sleep(80, "tax-calculation", e.listingId());
        return switch (e.region() == null ? "US" : e.region()) {
            case "UK" -> 0.20; case "DE" -> 0.19; default -> 0.08;
        };
    }

    private boolean checkCompliance(ListingEvent e) {
        sleep(60, "compliance-check", e.listingId());
        return true;
    }

    private String processImages(ListingEvent e) {
        sleep(120, "image-processing", e.listingId());
        return "https://cdn.ebay.com/listings/" + e.listingId() + "/main.jpg";
    }

    private void sleep(long ms, String task, String listingId) {
        try {
            Thread.sleep(ms);
            log.debug("[{}] {} done in {}ms thread={}", listingId, task, ms,
                    Thread.currentThread().getName());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Enrichment interrupted: " + task);
        }
    }

    public record EnrichmentResult(boolean categoryValid, double taxRate,
                                   boolean compliant, String processedImageUrl) {
        public boolean allPassed() { return categoryValid && compliant; }
    }
}
