package com.coresys.state.banking.domain;

import com.coresys.common.events.banking.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface PaymentRepository extends JpaRepository<PaymentEntity, Long> {
    Optional<PaymentEntity> findByPaymentId(String paymentId);
    List<PaymentEntity> findByStatusInAndUpdatedAtBefore(List<PaymentStatus> statuses, Instant cutoff);
    long countByStatus(PaymentStatus status);
    List<PaymentEntity> findByUserIdOrderByCreatedAtDesc(String userId);
}
