package com.coresys.state.domain;

import jakarta.persistence.*;

import java.time.Instant;

/**
 * Consumer-side dedup ledger (exactly-once approximation).
 * INSERT of a seen eventId violates the PK -> handler skips the event.
 * Insert happens in the SAME transaction as the state write: atomic.
 */
@Entity
@Table(name = "processed_events")
public class ProcessedEventEntity {

    @Id
    private String eventId;

    @Column(nullable = false)
    private Instant processedAt;

    protected ProcessedEventEntity() {}

    public ProcessedEventEntity(String eventId) {
        this.eventId = eventId;
        this.processedAt = Instant.now();
    }
}
