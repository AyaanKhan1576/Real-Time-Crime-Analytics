# Real-Time Crime Analytics and Intelligent Alert System

Big Data Engineering project using Chicago public safety datasets with Lambda Architecture.

## What Runs

The project uses Option B: Spark batch processing and Kafka + Storm streaming run at the same time.

```text
Historical CSVs -> PySpark -> PostgreSQL batch tables -> Streamlit historical views
Crime CSV replay -> Kafka -> Storm -> PostgreSQL realtime tables + MongoDB alert_logs -> Streamlit live views
```

Spark owns historical analytics. Kafka + Storm own realtime alerts and district counts. The dashboard reads both without duplicating analytics.

For sex offender density, the source dataset does not provide a police district field. Spark derives district where possible from matched crime blocks, keeps unmatched records separate, and does not rank unmatched records as a real district.

## Prerequisites

- Windows with WSL2 Ubuntu.
- Docker Desktop installed on Windows.
- Docker Desktop WSL integration enabled for Ubuntu.
- Git.
- No manual Windows install is needed for Spark, Kafka, Storm, PostgreSQL, or MongoDB.

Verify from WSL:

```bash
docker --version
docker compose version
docker ps
```

## Teammate Setup

Run all commands from the repo root in WSL:

```bash
cd /home/ayaan/University/Big_Data_Analytics/Project/Real-time-crime-analytics
```

For a teammate, replace that path with their own WSL repo path.

The root [docker-compose.yml](./docker-compose.yml) is the source of truth. [docker/docker-compose.yml](./docker/docker-compose.yml) only includes the root file for backward compatibility.

Host paths are configured through a local `.env` file. Create it only if the repo or dataset directory differs from the defaults:

```bash
cp .env.example .env
```

Then edit:

```dotenv
CRIME_ANALYTICS_PROJECT_ROOT=.
CRIME_ANALYTICS_DATA_DIR=./data
```

Rules for paths:

- Use WSL/Linux paths, not Windows `C:\...` paths.
- Do not hardcode personal paths in `docker-compose.yml`, `Makefile`, or scripts.
- Inside containers, the project is mounted at `/app`.
- Inside containers, datasets are mounted at `/app/data`.
- Keep `config/config.yaml` using `data.base_path: "/app/data"`.

## Datasets

Place the five CSVs in `data/`:

```text
data/crimes.csv
data/arrests.csv
data/violence.csv
data/sex_offenders.csv
data/police_stations.csv
```

The loader also falls back to the downloaded City of Chicago filenames, such as:

```text
Crimes_-_2001_to_Present_20260501.csv
Arrests_20260501.csv
Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260501.csv
Sex_Offenders_20260501.csv
Police_Stations_20260501.csv
```

## Configuration

Main full-run config:

```text
config/config.yaml
```

Fast Spark test config:

```text
config/config.spark_15pct.yaml
```

`config/config.yaml` is the default and uses the full dataset for Spark. `config/config.spark_15pct.yaml` limits large Spark inputs to about 15 percent for local testing. Kafka still streams the full configured crime CSV unless you pass an explicit producer row limit.

Important values:

```yaml
data:
  base_path: "/app/data"

kafka:
  topic_crime_events: "crime-events"
  producer_interval_seconds: 1

storm:
  window_size_seconds: 300
  slide_interval_seconds: 60
  anomaly_threshold: 25
```

To replay slower, edit `producer_interval_seconds` to `15` or `30`.

## Run Everything

Fast integrated test:

```bash
make run-all-15pct
```

Full project run:

```bash
make run-all
```

These targets start infrastructure, MongoDB indexes, Storm topology, Kafka producer, Spark batch processing, and Streamlit.

Open:

```text
Dashboard: http://localhost:8501
Spark UI:  http://localhost:8081
Storm UI:  http://localhost:8088
```

## Component Runs

Start shared services:

```bash
make up-detached
```

Spark only:

```bash
make spark-up
make spark-run-15pct
make run-dashboard
```

Full Spark run:

```bash
make spark-up
make spark-run
make run-dashboard
```

Streaming only:

```bash
make up-detached
make setup-mongo
make run-storm
make run-producer
make run-dashboard
```

Direct scripts are also available. Prefer `make` for normal runs because it keeps the command sequence consistent:

```bash
./scripts/run_spark_batch.sh /app/config/config.yaml
./scripts/run_spark_batch.sh /app/config/config.spark_15pct.yaml
./scripts/run_storm_topology.sh
./scripts/run_kafka_producer.sh
./scripts/run_dashboard.sh
```

## Logs

```bash
make logs-spark
make logs-producer
make logs-storm
make logs-dashboard
```

Spark run logs are written under:

```text
logs/spark/
```

## Verification

Batch status:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -c "
SELECT run_id, job_name, status, started_at, finished_at, message
FROM batch_job_status
ORDER BY created_at DESC
LIMIT 5;
"
```

Latest completed Spark output counts:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -c "
WITH latest AS (
  SELECT run_id
  FROM batch_job_status
  WHERE status = 'completed'
  ORDER BY finished_at DESC
  LIMIT 1
)
SELECT 'crime_trends' AS table_name, COUNT(*) FROM crime_trends WHERE run_id = (SELECT run_id FROM latest)
UNION ALL SELECT 'arrest_rates', COUNT(*) FROM arrest_rates WHERE run_id = (SELECT run_id FROM latest)
UNION ALL SELECT 'violence_stats', COUNT(*) FROM violence_stats WHERE run_id = (SELECT run_id FROM latest)
UNION ALL SELECT 'sex_offender_density', COUNT(*) FROM sex_offender_density WHERE run_id = (SELECT run_id FROM latest)
UNION ALL SELECT 'hotspots', COUNT(*) FROM hotspots WHERE run_id = (SELECT run_id FROM latest)
UNION ALL SELECT 'correlations', COUNT(*) FROM correlations WHERE run_id = (SELECT run_id FROM latest);
"
```

Storm topology:

```bash
docker exec storm-nimbus storm list
```

Realtime PostgreSQL output:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -c "
SELECT district, window_start, window_end, event_count, updated_at
FROM realtime_district_counts
ORDER BY updated_at DESC
LIMIT 10;
"

docker exec postgres psql -U crime_user -d crime_analytics -c "
SELECT alert_id, district, alert_timestamp, event_count, threshold_value, severity
FROM alerts
ORDER BY alert_timestamp DESC
LIMIT 10;
"
```

MongoDB alert logs:

```bash
docker exec mongodb mongosh crime_analytics --quiet --eval "db.alert_logs.find().sort({timestamp:-1}).limit(5).toArray()"
```

Dashboard health:

```bash
curl -I http://localhost:8501
```

## Stop

Stop containers and keep database volumes:

```bash
make down
```

Delete containers and database volumes:

```bash
docker compose down -v
```

`-v` deletes PostgreSQL and MongoDB stored results.

## Implementation Rules

- Spark uses explicit `StructType` schemas only.
- `inferSchema=True` must not be used.
- `district` is always a string; numeric districts are zero-padded, for example `001`.
- Spark writes temp tables first, then publishes final tables after success.
- Dashboard reads only the latest completed Spark `run_id`.
- Kafka and Storm do not run historical Spark analytics.

## Documentation

Detailed architecture, test commands, expected results, and troubleshooting notes are in:

```text
REPORT.md
instructions.md
TESTING.md
```
