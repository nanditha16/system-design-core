package com.coresys.processing.routing;

import com.coresys.common.events.TransactionEvent;
import com.coresys.common.events.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Business logic + routing seam.
 * Real systems branch here by region / product / risk score.
 * Keep this class as the "plug in domain logic" point in interviews.
 */
@Component
public class TransactionRouter {

    private static final Logger log = LoggerFactory.getLogger(TransactionRouter.class);

    public TransactionEvent process(TransactionEvent event) {
        // Domain validation / enrichment / routing decision goes here.
        if (event.amount() == null || event.amount().signum() <= 0) {
            throw new IllegalArgumentException("Invalid amount for " + event.transactionId());
        }
        log.info("Routing txn={} region={} type={}", event.transactionId(), event.region(), event.type());
        return event.withStatus(TransactionStatus.SENT);
    }
}
