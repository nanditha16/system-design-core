package com.coresys.state.brokerage.service;

import com.coresys.common.events.brokerage.BrokerageTopics;
import com.coresys.common.events.brokerage.OrderEvent;
import com.coresys.common.events.brokerage.OrderStatus;
import com.coresys.common.events.brokerage.OrderType;
import com.coresys.state.brokerage.domain.*;
import com.coresys.state.domain.ProcessedEventEntity;
import com.coresys.state.domain.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class OrderStateService {

    private static final Logger log = LoggerFactory.getLogger(OrderStateService.class);

    private final OrderRepository orders;
    private final AccountRepository accounts;
    private final ProcessedEventRepository processedEvents;
    private final HoldingRepository holdings;
    private final KafkaTemplate<String, Object> kafka;

    public OrderStateService(OrderRepository orders, AccountRepository accounts,
                             ProcessedEventRepository processedEvents,
                             HoldingRepository holdings,
                             KafkaTemplate<String, Object> kafka) {
        this.orders = orders;
        this.accounts = accounts;
        this.processedEvents = processedEvents;
        this.holdings = holdings;
        this.kafka = kafka;
    }

    @Transactional
    public void apply(OrderEvent event) {
        if (processedEvents.existsById(event.eventId())) {
            log.info("Duplicate event {} skipped (already applied)", event.eventId());
            return;
        }
        processedEvents.save(new ProcessedEventEntity(event.eventId()));

        try {
            AccountEntity account = accounts.findById(event.userId())
                    .orElseThrow(() -> new IllegalStateException("Account not found: " + event.userId()));

            if (event.type() == OrderType.BUY) {
                account.debit(event.totalAmount());
                holdings.findByUserIdAndSymbol(event.userId(), event.symbol())
                        .ifPresentOrElse(
                            h -> h.addShares(event.quantity()),
                            () -> holdings.save(new HoldingEntity(
                                    event.userId(), event.symbol(), event.quantity())));
            } else {
                HoldingEntity holding = holdings.findByUserIdAndSymbol(event.userId(), event.symbol())
                        .orElseThrow(() -> new IllegalStateException(
                                "No holding found: " + event.symbol() + " user=" + event.userId()));
                holding.removeShares(event.quantity());
                account.credit(event.totalAmount());
            }

            orders.save(new OrderEntity(
                    event.orderId(), event.userId(), event.idempotencyKey(),
                    event.type(), event.symbol(),
                    event.quantity(), event.price(), OrderStatus.EXECUTED));

            log.info("Order EXECUTED: {} {} {} qty={} price={} | new balance={}",
                    event.type(), event.symbol(), event.orderId(),
                    event.quantity(), event.price(), account.getBalance());

        } catch (IllegalStateException e) {
            // Serialize to JSON string — state-service producer uses StringSerializer
            String payload = String.format(
                    "{\"orderId\":\"%s\",\"userId\":\"%s\",\"symbol\":\"%s\"," +
                    "\"type\":\"%s\",\"amount\":\"%s\",\"reason\":\"%s\",\"rejectedAt\":\"%s\"}",
                    event.orderId(), event.userId(), event.symbol(),
                    event.type().name(), event.totalAmount(),
                    e.getMessage().replace("\"", "'"),
                    Instant.now());
            kafka.send(BrokerageTopics.ORDERS_REJECTED, event.orderId(), payload);
            log.warn("Order REJECTED {}: {}", event.orderId(), e.getMessage());

        } catch (DataIntegrityViolationException e) {
            log.warn("Idempotent skip order={} idem={}: {}",
                    event.orderId(), event.idempotencyKey(), e.getMessage());
        }
    }
}
