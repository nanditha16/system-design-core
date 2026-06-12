# system-design-core

One reusable, production-grade microservices skeleton for system design interviews.
Java 17 + Spring Boot 3 + WebFlux + Kafka + Redis + PostgreSQL.

```
Client -> api-gateway (8080)
            -> ingestion-service (8081)   [WebFlux, Redis idempotency, Kafka producer]
                 -> transactions.incoming.v1
            -> processing-service (8082)  [consumer, routing, retries, DLQ]
                 -> transactions.processed.v1
            -> state-service (8083)       [single writer, ACID, state machine, dedup]
            -> reconciliation-service (8084) [scheduled batch, discrepancy events]
```

## Quick start (Codespaces or local)

```bash
# 1. Infra (Kafka + ZK + Redis + Postgres)
docker compose -f infrastructure/docker-compose.yml up -d

# 2. Topics
./infrastructure/create-topics.sh

# 3. Build everything
mvn -q -DskipTests package

# 4. Run services (separate terminals, or use & )
mvn -pl services/api-gateway spring-boot:run
mvn -pl services/ingestion-service spring-boot:run
mvn -pl services/processing-service spring-boot:run
mvn -pl services/state-service spring-boot:run
mvn -pl services/reconciliation-service spring-boot:run

# 5. Smoke test (or open requests.http in VS Code)
curl -X POST localhost:8080/api/v1/transactions \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: k1' \
  -d '{"accountId":"a1","amount":100,"currency":"USD","region":"US","type":"PAYMENT"}'
```

In Codespaces the devcontainer auto-installs Java 17 + Maven, starts infra, and compiles on create.

## Structural feature flags (the interview switchboard)

These are conceptual modules toggled by config, not runtime feature-flag tooling.
The repo structure never changes; capabilities activate or deactivate.

| Module | Where | Toggle | Effect when OFF |
|---|---|---|---|
| kafka-enabled | ingestion `feature/publish`, processing `consumer` | `features.async.enabled=false` | Kafka removed; ingestion -> processing over REST (`SyncRestPublisher` + `SyncProcessingController`) |
| redis-idempotency | ingestion `feature/idempotency` | `features.idempotency.enabled=false` | Redis removed; `NoOpIdempotencyService` activates |
| reconciliation-batch | reconciliation-service | don't start the service / cron change | No EOD validation; real-time only |
| external-integration | reconciliation `ReconciliationJob` (documented seam) | add settlement-file join | Internal-only consistency checks |
| simple-sync-mode | both flags above off | both flags | Three-service REST chain, lightweight DB |

## Exactly-once (approximation), layered

1. API layer: Redis SETNX on `Idempotency-Key` (24h TTL) -> duplicate POSTs get 409
2. Producer: `enable.idempotence=true`, `acks=all` -> no broker-side dupes on retry
3. Consumer: manual ack, offset committed only AFTER DB transaction commits
4. DB: `processed_events` eventId ledger inserted in the SAME transaction as the state write
5. Backstop: `UNIQUE(transaction_id)`, `UNIQUE(idempotency_key)` constraints

Net: at-least-once delivery + idempotent apply = effectively-once.

## Failure handling

- Exponential backoff in processing-service (1s -> 2s -> 4s) via `DefaultErrorHandler`
- Exhausted retries -> `transactions.dlq.v1`
- Replay: ops consumer re-publishes DLQ records to incoming (dedup ledger absorbs duplicates)
- Stuck transactions (PENDING/SENT past SLA) -> reconciliation flags to `reconciliation.discrepancies.v1`

## State machine

`PENDING -> SENT -> SUCCESS | FAILED`, enforced in `TransactionEntity.transitionTo()`.
Terminal states immutable; illegal transitions throw inside the ACID transaction.

See `INTERVIEW_PLAYBOOK.md` for the 10-minute narrative and per-problem configurations.
