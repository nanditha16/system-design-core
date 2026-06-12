package com.coresys.processing.config;

import com.coresys.common.events.Topics;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Failure handling:
 *  - Exponential backoff: 1s -> 2s -> 4s (max 3 attempts)
 *  - Exhausted -> Dead Letter Queue (transactions.dlq.v1)
 *  - Replay = consumer on DLQ topic re-publishing to incoming (ops-triggered)
 */
@Configuration
@ConditionalOnProperty(name = "features.async.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaErrorConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, ex) -> new TopicPartition(Topics.TRANSACTIONS_DLQ, record.partition()));

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxElapsedTime(10_000L);

        return new DefaultErrorHandler(recoverer, backOff);
    }
}
