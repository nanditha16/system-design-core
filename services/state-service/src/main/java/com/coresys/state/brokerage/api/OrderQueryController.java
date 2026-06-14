package com.coresys.state.brokerage.api;

import com.coresys.state.brokerage.domain.*;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/brokerage")
public class OrderQueryController {

    private final OrderRepository orders;
    private final AccountRepository accounts;
    private final HoldingRepository holdings;

    public OrderQueryController(OrderRepository orders, AccountRepository accounts,
                                HoldingRepository holdings) {
        this.orders = orders;
        this.accounts = accounts;
        this.holdings = holdings;
    }

    @GetMapping("/orders/{orderId}")
    public ResponseEntity<OrderEntity> getOrder(@PathVariable String orderId) {
        return orders.findByOrderId(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/accounts/{userId}")
    public ResponseEntity<AccountEntity> getAccount(@PathVariable String userId) {
        return accounts.findById(userId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/accounts/{userId}/orders")
    public List<OrderEntity> getOrderHistory(@PathVariable String userId) {
        return orders.findByUserIdOrderByUpdatedAtDesc(userId);
    }

    @GetMapping("/accounts/{userId}/holdings")
    public List<HoldingEntity> getHoldings(@PathVariable String userId) {
        return holdings.findByUserId(userId);
    }
}
