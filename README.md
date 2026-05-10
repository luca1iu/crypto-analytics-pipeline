# Crypto On-Chain Analytics Pipeline

Real-time Ethereum + Base transaction analytics pipeline.

## Architecture
Alchemy WebSocket → Kafka → Spark Structured Streaming → BigQuery → dbt → Looker Studio

## Stack
- Python 3.10+
- Apache Kafka (Docker)
- Apache Spark Structured Streaming
- Google BigQuery
- dbt Core
- Looker Studio

## Setup
\`\`\`bash
cp .env.example .env
# Add your Alchemy API keys to .env
docker-compose -f infra/docker-compose.yml up -d
\`\`\`

## Status
🚧 In progress