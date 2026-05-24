#!/bin/bash
# Wait for Kafka to be fully ready
echo "Waiting for Kafka to be ready..."
sleep 10

# Create ETH raw transactions topic
docker exec kafka kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --topic eth_raw_transactions \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# Create Base raw transactions topic
docker exec kafka kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --topic base_raw_transactions \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Topics created. Listing all topics:"
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092