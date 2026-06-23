package com.coresys.state.ebay.domain;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;
public interface ListingRepository extends JpaRepository<ListingEntity, String> {
    List<ListingEntity> findBySellerIdOrderByCreatedAtDesc(String sellerId);
    List<ListingEntity> findBySellerIdAndStatus(String sellerId, String status);
    Optional<ListingEntity> findByIdempotencyKey(String key);
    long countBySellerIdAndStatus(String sellerId, String status);
}
