package com.coresys.processing.consumer;

import com.coresys.common.events.TransactionEvent;
import com.coresys.common.events.Topics;
import com.coresys.processing.routing.TransactionRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * FEATURE MODULE: kafka-enabled.
 * Manual ack mode: offset committed ONLY after successful downstream publish.
 * Failures -> DefaultErrorHandler -> exponential backoff retries -> DLQ.
 */
@Component
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    private final TransactionRouter router;
    private final KafkaTemplate<String, TransactionEvent> kafka;

    public TransactionConsumer(TransactionRouter router, KafkaTemplate<String, TransactionEvent> kafka) {
        this.router = router;
        this.kafka = kafka;
    }

    @KafkaListener(topics = Topics.TRANSACTIONS_INCOMING, groupId = "processing-service")
    public void onTransaction(TransactionEvent event, Acknowledgment ack) {
        TransactionEvent processed = router.process(event);
        kafka.send(Topics.TRANSACTIONS_PROCESSED, processed.transactionId(), processed)
             .join(); // ensure downstream publish before committing offset
        ack.acknowledge();
        log.info("Processed txn={} -> {}", processed.transactionId(), processed.status());
    }
}
