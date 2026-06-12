package com.coresys.common.events;

/**
 * Explicit transaction state machine:
 * PENDING -> SENT -> SUCCESS | FAILED
 * Transitions are enforced in state-service (single writer).
 */
public enum TransactionStatus {
    PENDING, SENT, SUCCESS, FAILED;

    public boolean canTransitionTo(TransactionStatus next) {
        return switch (this) {
            case PENDING -> next == SENT || next == FAILED;
            case SENT -> next == SUCCESS || next == FAILED;
            case SUCCESS, FAILED -> false; // terminal
        };
    }
}
