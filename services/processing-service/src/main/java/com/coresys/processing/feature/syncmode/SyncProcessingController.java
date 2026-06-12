package com.coresys.processing.feature.syncmode;

import com.coresys.common.events.TransactionEvent;
import com.coresys.processing.routing.TransactionRouter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

/**
 * FEATURE MODULE: simple-sync-mode.
 * Active only when features.async.enabled=false.
 * ingestion -> (REST) -> here -> (REST) -> state-service. No Kafka anywhere.
 */
@RestController
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "false")
public class SyncProcessingController {

    private final TransactionRouter router;

    public SyncProcessingController(TransactionRouter router) {
        this.router = router;
    }

    @PostMapping("/internal/process")
    public Mono<TransactionEvent> process(@RequestBody TransactionEvent event) {
        return Mono.fromCallable(() -> router.process(event));
        // In full sync mode, forward to state-service via WebClient here.
    }
}
