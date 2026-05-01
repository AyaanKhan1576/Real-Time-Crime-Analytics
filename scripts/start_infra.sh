#!/usr/bin/env bash
# Start infra using docker-compose
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo "Starting infra via docker-compose..."
docker-compose -f "$ROOT_DIR/docker/docker-compose.yml" up -d
echo "Services starting. Use 'docker-compose -f docker/docker-compose.yml logs -f' to follow logs."
