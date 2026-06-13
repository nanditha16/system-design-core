#!/usr/bin/env bash
# Explicit topic creation. Partition counts are the scaling unit:
# bump partitions -> add consumer instances -> linear throughput scaling.
set -euo pipefail

BROKER=kafka:29092

echo "Waiting for Kafka..."
until docker exec kafka kafka-broker-api-versions --bootstrap-server "$BROKER" > /dev/null 2>&1; do
  sleep 2
done
echo "Kafka ready."

create() {
  docker exec kafka kafka-topics --bootstrap-server "$BROKER" \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}

create transactions.incoming.v1        6
create transactions.processed.v1       6
create transactions.dlq.v1             3
create reconciliation.discrepancies.v1 3

echo "Topics:"
docker exec kafka kafka-topics --bootstrap-server "$BROKER" --list
