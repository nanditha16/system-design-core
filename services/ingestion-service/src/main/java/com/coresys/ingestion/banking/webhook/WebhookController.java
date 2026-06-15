package com.coresys.ingestion.banking.webhook;

import com.coresys.common.events.banking.*;
import com.coresys.ingestion.feature.publish.BankingEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/**
 * Core Banking webhook receiver.
 * Core banking POSTs SUCCESS/FAILURE callbacks here after processing.
 *
 * Security: In production, validate HMAC signature header from core banking.
 * mTLS seam: the connection FROM processing-service TO core banking uses mTLS
 * (see CoreBankingClient). This webhook is the callback path back.
 *
 * Publishes PaymentSucceededEvent or PaymentFailedEvent to banking.payment.events.v1
 * which fans out to 3 consumer groups:
 *   1. State manager  -> DB update
 *   2. Recovery engine -> retry on failure
 *   3. Audit sink     -> S3 data lake
 */
@RestController
@RequestMapping("/api/v1/banking/webhook")
public class WebhookController {

    private static final Logger log = LoggerFactory.getLogger(WebhookController.class);
    private final BankingEventPublisher publisher;

    public WebhookController(BankingEventPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping("/payment-result")
    public Mono<ResponseEntity<Void>> handleResult(@RequestBody WebhookRequest webhook) {
        log.info("Webhook received: paymentId={} result={}", webhook.paymentId(), webhook.result());

        PaymentStatus status = "SUCCESS".equalsIgnoreCase(webhook.result())
                ? PaymentStatus.CONFIRMED
                : PaymentStatus.FAILED;

        PaymentEvent event = new PaymentEvent(
                UUID.randomUUID().toString(),
                webhook.paymentId(),
                null,
                null,
                null,
                null,
                null,
                null,
                status,
                webhook.coreBankingRef(),
                Instant.now());

        return publisher.publishEvent(event)
                .map(e -> ResponseEntity.<Void>ok().build());
    }
}
