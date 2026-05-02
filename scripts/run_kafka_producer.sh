#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

docker-compose --profile jobs run --rm kafka-producer "$@"
