# Interview Playbook

One repo, many designs. You never change the structure; you activate or deactivate capabilities.

## The 10-minute walkthrough (default: full payment system)

1. **Edge (1 min)**: gateway splits writes (ingestion) from reads (state). Rate limiting and auth are gateway filters.
2. **Write path (3 min)**: ingestion is reactive (WebFlux) because it's I/O-bound fan-in at 100M-user scale. Redis SETNX dedups the idempotency key before anything touches Kafka. Publish PENDING, return 202; the client contract is async.
3. **Processing (2 min)**: consumer group scales horizontally with partitions. Routing seam (`TransactionRouter`) is where domain logic plugs in. Failures: exponential backoff, then DLQ, replay is dedup-safe.
4. **State (2 min)**: single-writer service owns Postgres. One ACID transaction = eventId dedup insert + state transition. Offset commits only after DB commit. Unique constraints are the last line of defense.
5. **Reconciliation (1 min)**: scheduled scan for non-terminal transactions past SLA; discrepancies published for ops. Cron it for EOD.
6. **Close (1 min)**: "exactly-once is an approximation: at-least-once delivery + idempotent apply at every hop."

## Case configurations

### Case 1: Full payment system (default)
Everything on. Emphasize: idempotency layers, offset-after-write, DLQ + replay, reconciliation.

### Case 2: Simple microservice orchestration
Set `features.async.enabled=false` and `features.idempotency.enabled=false`.
Say: "This problem doesn't justify event-driven complexity. Ingestion calls processing over REST; the abstraction (`EventPublisher`) means the controller code is identical."
Now the design is: gateway -> ingestion -> processing -> state, all synchronous.

### Case 3: Brokerage / low-latency reads
Keep the write path. Add: "I'd put a Redis read-through cache in front of `TransactionQueryController` with short TTL + event-driven invalidation from `transactions.processed.v1`. Reads scale on replicas; writes stay strongly consistent on the primary."

### Case 4: Banking EOD reconciliation
Lead with reconciliation-service. Switch `@Scheduled(fixedDelay)` to cron `0 0 2 * * *`, add the external settlement-file join (external-integration seam in `ReconciliationJob`).

### Case 5: Notification / audit fan-out
"Any new consumer group on `transactions.processed.v1` gets the full stream with zero producer changes." That sentence IS the event-driven payoff.

## Scaling answers (memorize)

- **Throughput**: partitions are the unit. 6 partitions -> up to 6 consumer instances per group. Need more: repartition + scale stateless services horizontally.
- **Hot DB**: single-writer state-service first vertical + read replicas; then shard by region/accountId (transactionId already keys partitions).
- **Burst traffic**: Kafka IS the buffer. Ingestion absorbs at wire speed; processing drains at its own pace. Backpressure is free.
- **100M users**: stateless services behind the gateway autoscale; Redis clusters; the only careful scaling story is Postgres (shard or move hot state to a partitioned ledger table).

## Trade-off answers (memorize)

- WebFlux at ingestion (I/O fan-in) but blocking JPA at state-service (ACID writes; reactive buys nothing, costs transaction ergonomics).
- Kafka adds latency + ops cost; justified by decoupling, replay, and burst absorption. Say when you'd NOT use it (Case 2).
- Shared local Postgres for recon is a simplification; production reads a replica/CDC read model.
- Outbox pattern is the next rigor step: write event + state in one transaction, relay publishes. Mention it when asked "what if Kafka publish fails after DB commit?"
