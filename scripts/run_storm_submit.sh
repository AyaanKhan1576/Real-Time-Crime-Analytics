#!/usr/bin/env bash
set -euo pipefail

docker compose -f docker/docker-compose.yml --profile java-submit up -d storm-submit-java