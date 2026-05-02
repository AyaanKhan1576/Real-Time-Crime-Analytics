#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG_PATH="${1:-/app/config/config.yaml}"
shift || true

mkdir -p logs/spark
chmod 0777 logs logs/spark 2>/dev/null || true

docker compose -f docker-compose.yml exec spark-master \
  /opt/spark/bin/spark-submit /app/spark/run_batch.py --config "$CONFIG_PATH" "$@"
