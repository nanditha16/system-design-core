package com.coresys.common.events.ebay;

/**
 * Inventory is event-driven. State is reconstructed from event log.
 *
 * Example order lifecycle:
 *   RESERVE 5  → available=95, reserved=5
 *   SELL 5     → reserved=0, sold=5
 *   (or RELEASE 5 on order cancellation → available=100, reserved=0)
 */
public enum InventoryEventType { ADD, REMOVE, RESERVE, RELEASE, SELL }
