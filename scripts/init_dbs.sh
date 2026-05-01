#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Waiting for Postgres to be ready..."
until docker exec $(docker ps -q -f ancestor=postgres:14) pg_isready -U "$POSTGRES_USER" >/dev/null 2>&1; do
  sleep 1
done

echo "Initializing Postgres schema..."
# execute all SQL files in db/ via docker-entrypoint-initdb.d already mounted by compose; but for running against existing container:
PG_CONTAINER=$(docker ps -q -f ancestor=postgres:14)
if [ -n "$PG_CONTAINER" ]; then
  docker exec -i $PG_CONTAINER psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/postgres_init.sql || true
fi

echo "Initializing Mongo indexes..."
MONGO_CONTAINER=$(docker ps -q -f ancestor=mongo:6)
if [ -n "$MONGO_CONTAINER" ]; then
  docker exec -i $MONGO_CONTAINER bash -lc "python3 - <<'PY'
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.get_database('crime_analytics')
db.alert_logs.create_index('alert_id', unique=True)
print('Mongo indexes ensured')
PY"
fi

echo "DB init complete."
