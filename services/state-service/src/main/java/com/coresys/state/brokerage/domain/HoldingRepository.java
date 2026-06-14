package com.coresys.state.brokerage.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;

public interface HoldingRepository extends JpaRepository<HoldingEntity, Long> {
    Optional<HoldingEntity> findByUserIdAndSymbol(String userId, String symbol);
    List<HoldingEntity> findByUserId(String userId);
}
