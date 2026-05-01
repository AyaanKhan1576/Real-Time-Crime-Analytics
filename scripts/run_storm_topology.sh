#!/usr/bin/env bash
set -euo pipefail

echo "Start the real cluster first: docker compose -f docker/docker-compose.yml up -d"
echo "Then submit the Java topology with:"
echo "docker compose -f docker/docker-compose.yml --profile java-submit up -d storm-submit-java"
