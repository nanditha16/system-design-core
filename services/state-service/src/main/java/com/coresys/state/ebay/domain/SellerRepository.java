package com.coresys.state.ebay.domain;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
public interface SellerRepository extends JpaRepository<SellerEntity, String> {
    Optional<SellerEntity> findByEmail(String email);
}
