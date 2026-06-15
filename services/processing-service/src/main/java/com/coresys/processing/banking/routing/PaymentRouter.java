package com.coresys.processing.banking.routing;

import com.coresys.common.events.banking.PaymentEvent;
import com.coresys.common.events.banking.PaymentRegion;
import com.coresys.common.events.banking.PaymentStatus;
import com.coresys.processing.banking.corebanking.CoreBankingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/**
 * Routes payments to the correct core banking system based on region + type.
 *
 * Interview talking point: "The router is the seam for adding new rails.
 * Adding SEPA routing = add a new case here + a new CoreBankingClient impl.
 * No other service changes."
 *
 * Production routing logic:
 *  US + ACH/WIRE -> Fedwire / ACH network
 *  EU + SEPA     -> SWIFT / SEPA clearing
 *  APAC          -> Local clearing house
 */
@Component
public class PaymentRouter {

    private static final Logger log = LoggerFactory.getLogger(PaymentRouter.class);
    private final CoreBankingClient coreBankingClient;

    public PaymentRouter(CoreBankingClient coreBankingClient) {
        this.coreBankingClient = coreBankingClient;
    }

    public Mono<PaymentEvent> route(PaymentEvent event) {
        String rail = determineRail(event);
        log.info("Routing paymentId={} type={} region={} -> rail={}",
                event.paymentId(), event.type(), event.region(), rail);
        return coreBankingClient.send(event);
    }

    private String determineRail(PaymentEvent event) {
        if (event.region() == PaymentRegion.EU) return "SEPA/SWIFT";
        if (event.region() == PaymentRegion.APAC) return "LOCAL_CLEARING";
        return switch (event.type()) {
            case ACH         -> "ACH_NETWORK";
            case WIRE        -> "FEDWIRE";
            case CREDIT_CARD -> "CARD_NETWORK";
            default          -> "DEFAULT_RAIL";
        };
    }
}
