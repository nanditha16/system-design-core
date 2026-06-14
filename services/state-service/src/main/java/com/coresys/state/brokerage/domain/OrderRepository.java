package com.coresys.state.brokerage.domain;

import com.coresys.common.events.brokerage.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface OrderRepository extends JpaRepository<OrderEntity, Long> {
    Optional<OrderEntity> findByOrderId(String orderId);
    List<OrderEntity> findByUserIdOrderByUpdatedAtDesc(String userId);
    List<OrderEntity> findByStatusInAndUpdatedAtBefore(List<OrderStatus> statuses, Instant cutoff);
}
