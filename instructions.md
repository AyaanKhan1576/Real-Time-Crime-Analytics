# Instructions

Project path:

```text
/home/ayaan/University/Big_Data_Analytics/Project/Real-time-crime-analytics
```

## 1. Prerequisites

- Windows with WSL2 Ubuntu.
- Docker Desktop installed on Windows.
- Docker Desktop WSL integration enabled for Ubuntu.
- Git.
- VS Code is optional.
- No manual Windows install is needed for Kafka, Spark, Storm, PostgreSQL, or MongoDB.

Verify Docker from inside WSL:

```bash
docker --version
docker compose version
docker ps
```

## 2. Project Setup

Always work from WSL:

```bash
cd /home/ayaan/University/Big_Data_Analytics/Project/Real-time-crime-analytics
```

Root Compose file:

```text
docker-compose.yml
```

Compatibility wrapper:

```text
docker/docker-compose.yml
```

Validate Compose:

```bash
docker compose -f docker-compose.yml config
docker compose -f docker-compose.yml --profile streaming --profile java-submit --profile harness config --services
```

## 3. Configurable Local Paths

Compose uses relative paths by default. If your teammate has the repo or data somewhere else, copy:

```bash
cp .env.example .env
```

Then edit:

```dotenv
CRIME_ANALYTICS_PROJECT_ROOT=.
CRIME_ANALYTICS_DATA_DIR=./data
```

Use WSL/Linux paths for absolute values. Do not use Windows `C:\...` paths.

Inside containers, the data directory is stable:

```yaml
data:
  base_path: "/app/data"
```

Change dataset filenames and sample sizes in:

```text
config/config.yaml
config/config.spark_15pct.yaml
```

`config/config.yaml` is the full-run default. `config/config.spark_15pct.yaml`
is the faster local test config. It limits the large Spark datasets to about 15
percent of the current local CSV row counts; the police stations dimension is
kept complete so district joins remain meaningful.

## 4. Single-Command Integrated Runs

Recommended test run. Spark uses 15 percent data; Kafka streams the full crime CSV as simulated live data:

```bash
make run-all-15pct
```

Full run. Spark uses the full dataset:

```bash
make run-all
```

These targets start infrastructure, Mongo indexes, Java Storm topology, Kafka producer, Spark batch processing, and Streamlit.

Open:

```text
Dashboard: http://localhost:8501
Spark UI:  http://localhost:8081
Storm UI:  http://localhost:8088
```

## 5. Realtime Streaming Rate

Kafka producer settings live in `config/config.yaml`:

```yaml
kafka:
  producer_interval_seconds: 1
  producer_rate_per_second: 1
```

Current behavior is one crime event every `1` second, using all rows in the configured crime CSV. To slow the simulated live feed to one row every 15 or 30 seconds:

```yaml
kafka:
  producer_interval_seconds: 15
```

If `producer_interval_seconds` is removed or set to `0`, `producer_rate_per_second` is used.

## 6. Component-Level Runs

Start shared services:

```bash
make up-detached
```

Spark-only 15 percent test:

```bash
make spark-up
make spark-run-15pct
make run-dashboard
```

Spark-only full run:

```bash
make spark-up
make spark-run
make run-dashboard
```

Streaming-only run:

```bash
make up-detached
make setup-mongo
make run-storm
make run-producer
make run-dashboard
```

Direct scripts are also available:

```bash
./scripts/run_spark_batch.sh /app/config/config.spark_15pct.yaml
./scripts/run_storm_topology.sh
./scripts/run_kafka_producer.sh
./scripts/run_dashboard.sh
```

## 7. Logs

Spark progress:

```bash
make logs-spark
```

Kafka producer:

```bash
make logs-producer
```

Storm:

```bash
make logs-storm
```

Dashboard:

```bash
make logs-dashboard
```

## 8. Spark Verification

Latest batch status:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -c "
SELECT run_id, job_name, status, started_at, finished_at, message
FROM batch_job_status
ORDER BY created_at DESC
LIMIT 5;
"
```

Latest completed run counts:

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

## 9. Streaming Verification

Realtime district counts:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -c "
SELECT district, window_start, window_end, event_count, updated_at
FROM realtime_district_counts
ORDER BY updated_at DESC
LIMIT 10;
"
```

Alerts:

```bash
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

The alert table may remain empty until the district count threshold is crossed. The `realtime_district_counts` table should update first.

## 10. Dashboard Verification

```bash
curl -I http://localhost:8501
```

Expected:

```text
HTTP/1.1 200 OK
```

Dashboard sections:

- System Status
- Batch Job Status
- Real-Time Alerts
- Real-Time District Counts
- Historical Crime Trends
- Arrest Rate Analysis
- Violence Analysis
- Sex Offender Density
- Hotspot Map
- Correlations

## 11. Database Utilities

PostgreSQL health:

```bash
docker exec postgres pg_isready -U crime_user -d crime_analytics
```

PostgreSQL tables:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -Atc "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;"
```

Apply PostgreSQL schema manually:

```bash
docker exec -i postgres psql -U crime_user -d crime_analytics < db/postgres_init.sql
```

MongoDB health:

```bash
docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping')"
```

MongoDB setup:

```bash
make setup-mongo
```

## 12. Stopping and Cleaning

Stop containers, keep volumes:

```bash
make down
```

Full cleanup:

```bash
docker compose down -v
```

Warning: `-v` deletes PostgreSQL and MongoDB stored results.

## 13. Notes

- Spark uses explicit schemas only.
- `inferSchema=True` must not be used.
- `district` is standardized as a zero-padded string where numeric, for example `001`.
- Spark writes staging tables first, then publishes final tables after success.
- Dashboard reads only the latest completed Spark run for historical analytics.
- Kafka and Storm write only realtime alert/count tables.
- The root `docker-compose.yml` should be used for all new commands.
