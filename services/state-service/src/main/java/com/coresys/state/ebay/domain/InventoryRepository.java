package com.coresys.state.ebay.domain;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
public interface InventoryRepository extends JpaRepository<InventoryEntity, String> {
    List<InventoryEntity> findByListingIdIn(List<String> ids);
}
