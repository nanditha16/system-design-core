package com.coresys.state.domain;

import com.coresys.common.events.TransactionStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface Repositories {

    interface TransactionRepository extends JpaRepository<TransactionEntity, Long> {
        Optional<TransactionEntity> findByTransactionId(String transactionId);
        List<TransactionEntity> findByStatusInAndUpdatedAtBefore(List<TransactionStatus> statuses, Instant cutoff);
        long countByStatus(TransactionStatus status);
    }

    interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, String> {}
}
