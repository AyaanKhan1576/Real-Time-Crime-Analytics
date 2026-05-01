# Real-Time Crime Analytics and Intelligent Alert System

This repository contains the university Big Data Engineering project implementation.

## Person 2 Scope (Kafka + Storm Speed Layer)

Implemented components:
- `kafka/producer.py`: replays crime CSV rows as JSON to Kafka topic `crime-events`
- `storm/bolts/parse_bolt.py`: validates messages and drops malformed input
- `storm/bolts/district_bolt.py`: normalizes and routes district as string
- `storm/bolts/window_bolt.py`: sliding window district counts
- `storm/bolts/anomaly_bolt.py`: severity detection (`LOW`, `MEDIUM`, `HIGH`)
- `storm/bolts/alert_bolt.py`: writes
  - PostgreSQL `realtime_district_counts`
  - PostgreSQL `alerts`
  - MongoDB `alert_logs`
- `db/postgres_init.sql`: speed-layer PostgreSQL schema
- `db/mongo_setup.py`: MongoDB index setup including TTL for `raw_events`

## Config

`config/config.yaml` is ignored. Copy from example:

```bash
cp config/config.yaml.example config/config.yaml
```

You can override values via env variables using `__` separators:

```bash
export KAFKA__BOOTSTRAP_SERVERS=localhost:9092
export POSTGRES__HOST=localhost
```

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Validate

```bash
python scripts/validate_streaming.py
```

## Run Producer (local)

```bash
python kafka/producer.py --config config/config.yaml
```

Optional quick test with limited rows:

```bash
python kafka/producer.py --config config/config.yaml --max-rows 200
```

## Initialize MongoDB Indexes

```bash
python db/mongo_setup.py
```

## Makefile shortcuts

```bash
make validate-streaming
make run-producer
make run-mongo-setup
```

## Notes

- Kafka topic is fixed to `crime-events` via config.
- `district` is treated as string in producer and bolts.
- `alert_id` format is deterministic: `district_windowStart_windowEnd`.
- Raw event logging is disabled by default (`mongodb.enable_raw_event_logging=false`).
