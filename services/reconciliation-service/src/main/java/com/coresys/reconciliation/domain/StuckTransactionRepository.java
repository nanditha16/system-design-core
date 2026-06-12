package com.coresys.reconciliation.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface StuckTransactionRepository extends JpaRepository<StuckTransaction, Long> {

    @Query("SELECT t FROM StuckTransaction t WHERE t.status IN ('PENDING','SENT') AND t.updatedAt < :cutoff")
    List<StuckTransaction> findStuck(@Param("cutoff") Instant cutoff);
}
