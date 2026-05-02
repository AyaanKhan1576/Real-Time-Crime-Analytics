#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose -f docker-compose.yml --profile streaming up -d --build --force-recreate kafka-producer

echo "Kafka producer is streaming all configured crime rows in the background."
echo "Watch it with: docker compose -f docker-compose.yml logs -f kafka-producer"
