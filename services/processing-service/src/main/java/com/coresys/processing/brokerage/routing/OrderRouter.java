package com.coresys.processing.brokerage.routing;

import com.coresys.common.events.brokerage.OrderEvent;
import com.coresys.common.events.brokerage.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Brokerage routing seam.
 * Plug domain logic here: margin checks, risk limits, market-hours validation.
 * For now: validates order and transitions PENDING -> EXECUTING.
 */
@Component
public class OrderRouter {

    private static final Logger log = LoggerFactory.getLogger(OrderRouter.class);

    public OrderEvent process(OrderEvent event) {
        if (event.quantity() == null || event.quantity().signum() <= 0)
            throw new IllegalArgumentException("Invalid quantity for order " + event.orderId());
        if (event.price() == null || event.price().signum() <= 0)
            throw new IllegalArgumentException("Invalid price for order " + event.orderId());

        log.info("Routing order={} type={} symbol={} qty={} price={}",
                event.orderId(), event.type(), event.symbol(),
                event.quantity(), event.price());

        return event.withStatus(OrderStatus.EXECUTING);
    }
}
