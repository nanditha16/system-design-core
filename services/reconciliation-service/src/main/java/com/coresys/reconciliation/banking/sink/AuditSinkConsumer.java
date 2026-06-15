package com.coresys.reconciliation.banking.sink;

import com.coresys.common.events.banking.BankingTopics;
import com.coresys.common.events.banking.PaymentEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.context.annotation.Profile;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * CONSUMER GROUP 3: Audit / Data Lake Sink
 *
 * Writes payment events to partitioned local files (simulating S3/HDFS).
 * Production: replace file write with S3PutObject via AWS SDK.
 *
 * Partition strategy: /audit-lake/year=YYYY/month=MM/day=DD/event_type=X/
 * This mirrors S3 partition layout enabling Athena/Spark queries.
 *
 * Interview talking point: "Every event type gets its own partition.
 * EOD reconciliation reads PaymentCreated + PaymentConfirmed partitions,
 * joins on payment_id+amount, and flags mismatches."
 */
@Component
@Profile("banking")
public class AuditSinkConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuditSinkConsumer.class);
    private static final String LAKE_ROOT = "/tmp/audit-lake";

    @KafkaListener(topics = {BankingTopics.PAYMENTS_INCOMING, BankingTopics.PAYMENT_EVENTS},
                   groupId = "banking-audit-sink")
    public void onEvent(PaymentEvent event, Acknowledgment ack) {
        try {
            writeToLake(event);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Audit sink failed for event {}: {}", event.eventId(), e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void writeToLake(PaymentEvent event) throws IOException {
        LocalDate today = LocalDate.now();
        String partition = String.format("%s/year=%d/month=%02d/day=%02d/event_type=%s",
                LAKE_ROOT, today.getYear(), today.getMonthValue(),
                today.getDayOfMonth(), event.status().name());

        Path dir = Paths.get(partition);
        Files.createDirectories(dir);

        Path file = dir.resolve(event.eventId() + ".json");
        String json = String.format(
                "{\"eventId\":\"%s\",\"paymentId\":\"%s\",\"userId\":\"%s\"," +
                "\"amount\":\"%s\",\"currency\":\"%s\",\"type\":\"%s\"," +
                "\"region\":\"%s\",\"status\":\"%s\",\"coreBankingRef\":\"%s\"," +
                "\"occurredAt\":\"%s\"}",
                event.eventId(), event.paymentId(),
                event.userId() != null ? event.userId() : "",
                event.amount() != null ? event.amount() : "",
                event.currency() != null ? event.currency() : "",
                event.type() != null ? event.type() : "",
                event.region() != null ? event.region() : "",
                event.status(),
                event.coreBankingRef() != null ? event.coreBankingRef() : "",
                event.occurredAt());

        Files.writeString(file, json);
        log.info("Audit sink: wrote event={} status={} to {}", event.eventId(), event.status(), file);
    }
}
