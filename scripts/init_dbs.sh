#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"

POSTGRES_USER="${POSTGRES_USER:-crime_user}"
POSTGRES_DB="${POSTGRES_DB:-crime_analytics}"

echo "Waiting for Postgres to be ready..."
until docker-compose -f "$COMPOSE_FILE" exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
  sleep 1
done

echo "Initializing Postgres schema..."
docker-compose -f "$COMPOSE_FILE" exec -T postgres \
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/postgres_init.sql

echo "Initializing Mongo indexes..."
docker-compose -f "$COMPOSE_FILE" exec -T mongo bash -lc "python3 - <<'PY'
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.get_database('crime_analytics')
db.alert_logs.create_index('alert_id', unique=True)
print('Mongo indexes ensured')
PY"

echo "DB init complete."
