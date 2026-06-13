package com.coresys.state.api;

import com.coresys.state.domain.TransactionRepository;
import com.coresys.state.domain.TransactionEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Read API. FEATURE MODULE seam for "low-latency reads" interview variant:
 * put a Redis read-through cache in front of this repository call
 * (brokerage / portfolio-style hot reads).
 */
@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionQueryController {

    private final TransactionRepository repository;
    

    public TransactionQueryController(TransactionRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/{transactionId}")
    public ResponseEntity<TransactionEntity> get(@PathVariable String transactionId) {
        return repository.findByTransactionId(transactionId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
