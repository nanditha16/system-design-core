package com.coresys.common.events.instagram;

/**
 * STRUCTURAL FLAG: fan-out on write vs fan-out on read.
 * WRITE → push post into every follower's Redis timeline immediately.
 * READ  → mega-influencer path: skip fanout, assemble at read time.
 *         Applied when follower count > threshold (default 1M).
 */
public enum FanoutStrategy { WRITE, READ }
