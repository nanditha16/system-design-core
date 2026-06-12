package com.coresys.state.consumer;

import com.coresys.common.events.TransactionEvent;
import com.coresys.common.events.Topics;
import com.coresys.state.domain.TransactionStateService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CRITICAL ORDERING: DB commit FIRST, offset commit SECOND.
 * Crash between the two -> redelivery -> processed_events dedup absorbs it.
 * Never the reverse (offset first risks silent message loss).
 */
@Component
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class ProcessedEventConsumer {

    private final TransactionStateService stateService;

    public ProcessedEventConsumer(TransactionStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = Topics.TRANSACTIONS_PROCESSED, groupId = "state-service")
    public void onProcessed(TransactionEvent event, Acknowledgment ack) {
        stateService.apply(event);   // ACID write + dedup, commits here
        ack.acknowledge();           // offset commit AFTER DB write
    }
}
