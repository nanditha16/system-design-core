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

# Generic Payment ---"
# create transactions.incoming.v1        6
# create transactions.processed.v1       6
# create transactions.dlq.v1             3
# create reconciliation.discrepancies.v1 3

# Brokerage topics
# create orders.incoming.v1     6
# create orders.processed.v1    6
# create orders.dlq.v1          3
# create orders.discrepancies.v1 3
# create orders.rejected.v1 3

# # Banking reconciliation topics
# create banking.payments.incoming.v1   6
# create banking.payments.routed.v1     6
# create banking.payment.events.v1      6
# create banking.payments.dlq.v1        3
# create banking.payments.retry.v1      3
# create banking.recon.discrepancies.v1 3

# Instagram topics
create instagram.posts.incoming.v1 6
create instagram.posts.fanout.v1   6
create instagram.follow.events.v1  6
create instagram.feed.updates.v1   6
create instagram.dlq.v1            3
create instagram.recon.v1          3

echo "Topics:"
docker exec kafka kafka-topics --bootstrap-server "$BROKER" --list
