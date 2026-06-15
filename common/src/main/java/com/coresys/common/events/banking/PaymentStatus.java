package com.coresys.common.events.banking;

/**
 * Payment state machine:
 * PENDING -> SENT -> CONFIRMED | FAILED
 * SENT -> FAILED (core banking timeout / rejection)
 */
public enum PaymentStatus {
    PENDING, SENT, CONFIRMED, FAILED;

    public boolean canTransitionTo(PaymentStatus next) {
        return switch (this) {
            case PENDING -> next == SENT || next == FAILED;
            case SENT    -> next == CONFIRMED || next == FAILED;
            case CONFIRMED, FAILED -> false;
        };
    }
}
