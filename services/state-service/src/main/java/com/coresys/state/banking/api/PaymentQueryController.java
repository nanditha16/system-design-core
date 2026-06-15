package com.coresys.state.banking.api;

import com.coresys.state.banking.domain.PaymentEntity;
import com.coresys.state.banking.domain.PaymentRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Read API for banking payments.
 * FEATURE SEAM: add Redis cache for hot GET /payments/{id} reads.
 */
@RestController
@RequestMapping("/api/v1/banking/payments")
public class PaymentQueryController {

    private final PaymentRepository payments;

    public PaymentQueryController(PaymentRepository payments) {
        this.payments = payments;
    }

    @GetMapping("/{paymentId}")
    public ResponseEntity<PaymentEntity> get(@PathVariable String paymentId) {
        return payments.findByPaymentId(paymentId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/user/{userId}")
    public List<PaymentEntity> getByUser(@PathVariable String userId) {
        return payments.findByUserIdOrderByCreatedAtDesc(userId);
    }

    @GetMapping("/stats")
    public java.util.Map<String, Long> stats() {
        return java.util.Map.of(
                "pending",   payments.countByStatus(com.coresys.common.events.banking.PaymentStatus.PENDING),
                "sent",      payments.countByStatus(com.coresys.common.events.banking.PaymentStatus.SENT),
                "confirmed", payments.countByStatus(com.coresys.common.events.banking.PaymentStatus.CONFIRMED),
                "failed",    payments.countByStatus(com.coresys.common.events.banking.PaymentStatus.FAILED));
    }
}
