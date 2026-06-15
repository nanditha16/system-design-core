package com.coresys.state.consumer;

import com.coresys.common.events.TransactionEvent;
import com.coresys.common.events.Topics;
import com.coresys.state.domain.TransactionStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;

@Component
@Profile("generic")
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class ProcessedEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProcessedEventConsumer.class);
    private final TransactionStateService stateService;

    public ProcessedEventConsumer(TransactionStateService stateService) {
        this.stateService = stateService;
    }

    @KafkaListener(topics = Topics.TRANSACTIONS_PROCESSED, groupId = "state-service")
    public void onProcessed(TransactionEvent event, Acknowledgment ack) {
        try {
            stateService.apply(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to apply event {}: {}", event.eventId(), e.getMessage(), e);
            throw e;
        }
    }
}