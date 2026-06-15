package com.coresys.processing.banking.corebanking;

import com.coresys.common.events.banking.PaymentEvent;
import com.coresys.common.events.banking.PaymentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * mTLS seam: in production configure:
 *   spring.webflux.ssl.key-store=classpath:client-keystore.p12
 *   spring.webflux.ssl.trust-store=classpath:truststore.p12
 * The WebClient is then built with SslContext using the above certs.
 *
 * Locally: plain HTTP to a stub. Real mTLS cert setup is infra concern.
 *
 * Interview talking point: "mTLS ensures mutual authentication between
 * our routing service and core banking — both parties verify each other's
 * certificate, preventing man-in-the-middle attacks on the internal network."
 */
@Component
public class CoreBankingClient {

    private static final Logger log = LoggerFactory.getLogger(CoreBankingClient.class);

    private final WebClient client;

    public CoreBankingClient(
            @Value("${banking.core-banking-url:http://localhost:9090}") String baseUrl) {
        this.client = WebClient.builder()
                .baseUrl(baseUrl)
                // mTLS seam: .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    /**
     * Sends payment to core banking system.
     * Returns the updated event with coreBankingRef set on success.
     * Throws on failure — caller handles retry/DLQ.
     *
     * In real systems: core banking ACKs immediately (SENT),
     * then POSTs webhook when truly settled (CONFIRMED/FAILED).
     */
    public Mono<PaymentEvent> send(PaymentEvent event) {
        log.info("Sending to core banking: paymentId={} type={} region={} amount={}",
                event.paymentId(), event.type(), event.region(), event.amount());

        // Stub: simulate core banking ACK
        // Production: POST to core banking API with mTLS
        return Mono.just(
                event.withStatus(PaymentStatus.SENT)
                     .withCoreBankingRef("CORE-" + event.paymentId().substring(0, 8)))
                .delayElement(Duration.ofMillis(50)); // simulate network latency
    }
}
