#!/usr/bin/env bash
# Start infra using docker-compose
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "Starting infra via docker-compose..."
docker compose -f "$ROOT_DIR/docker-compose.yml" up -d --build postgres mongodb zookeeper kafka storm-nimbus storm-supervisor storm-ui spark-master spark-worker streamlit
echo "Services starting. Use 'docker compose -f docker-compose.yml logs -f' to follow logs."
