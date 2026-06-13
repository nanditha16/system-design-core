package com.coresys.state.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface TransactionRepository extends JpaRepository<TransactionEntity, Long> {
    Optional<TransactionEntity> findByTransactionId(String transactionId);
    List<TransactionEntity> findByStatusInAndUpdatedAtBefore(List<String> statuses, Instant cutoff);
    long countByStatus(String status);
}
