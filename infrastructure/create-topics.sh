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

# # Instagram topics
# create instagram.posts.incoming.v1 6
# create instagram.posts.fanout.v1   6
# create instagram.follow.events.v1  6
# create instagram.feed.updates.v1   6
# create instagram.dlq.v1            3
# create instagram.recon.v1          3

# eBay Seller Platform topics
create ebay.listings.incoming.v1   6   # DRAFT listings from mutations
create ebay.listings.enriched.v1   6   # after CompletableFuture enrichment pipeline
create ebay.inventory.events.v1    6   # ADD/RESERVE/SELL/RELEASE events
create ebay.seller.events.v1       3   # ONBOARDED/ACTIVATED seller events
create ebay.dlq.v1                 3   # enrichment failures, oversell retries exhausted
create ebay.recon.v1               3   # STUCK_DRAFT / INVENTORY_ZERO_ACTIVE discrepancies

echo "Topics:"
docker exec kafka kafka-topics --bootstrap-server "$BROKER" --list
