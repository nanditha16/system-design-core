package com.coresys.common.events.brokerage;
public enum OrderStatus {
    PENDING, EXECUTING, PARTIAL, EXECUTED, REJECTED, FAILED;

    public boolean canTransitionTo(OrderStatus next) {
        return switch (this) {
            case PENDING   -> next == EXECUTING || next == REJECTED;
            case EXECUTING -> next == EXECUTED  || next == PARTIAL || next == FAILED;
            case PARTIAL   -> next == EXECUTED  || next == FAILED;  // more fills arriving
            case EXECUTED, REJECTED, FAILED -> false;
        };
    }
}
