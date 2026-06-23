package com.coresys.state.ebay.service;

import com.coresys.common.events.ebay.SellerEvent;
import com.coresys.state.domain.ProcessedEventEntity;
import com.coresys.state.domain.ProcessedEventRepository;
import com.coresys.state.ebay.domain.SellerEntity;
import com.coresys.state.ebay.domain.SellerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SellerStateService {

    private static final Logger log = LoggerFactory.getLogger(SellerStateService.class);
    private final SellerRepository sellers;
    private final ProcessedEventRepository processedEvents;

    public SellerStateService(SellerRepository sellers, ProcessedEventRepository processedEvents) {
        this.sellers = sellers; this.processedEvents = processedEvents;
    }

    @Transactional
    public void apply(SellerEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate seller event {} skipped", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEventEntity(event.eventId()));

        try {
            sellers.save(new SellerEntity(event.sellerId(), event.businessName(),
                    event.email(), event.countryCode()));
            log.info("Seller onboarded: {} {}", event.sellerId(), event.email());
        } catch (DataIntegrityViolationException e) {
            log.warn("Idempotent skip seller={}", event.sellerId());
        }
    }
}
