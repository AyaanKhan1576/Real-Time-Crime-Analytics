#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG_PATH="${1:-/app/config/config.yaml}"
shift || true

docker compose -f docker-compose.yml exec spark-master \
  /opt/spark/bin/spark-submit /app/spark/run_batch.py --config "$CONFIG_PATH" "$@"
