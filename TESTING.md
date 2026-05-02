# Testing Guide

Run all commands from the repository root in WSL:

```bash
cd /home/ayaan/University/Big_Data_Analytics/Project/Real-time-crime-analytics
```

The root `docker-compose.yml` is the source of truth. The file under `docker/` is only a compatibility wrapper.

## 1. Compose Validation

```bash
docker compose -f docker-compose.yml --profile streaming --profile java-submit --profile harness config
docker compose -f docker-compose.yml --profile streaming --profile java-submit --profile harness config --services
```

Expected core services:

```text
postgres
mongodb
zookeeper
kafka
storm-nimbus
storm-supervisor
storm-ui
spark-master
spark-worker
streamlit
```

## 2. Fast Integrated Test

This is the recommended local verification command. Spark uses the 15 percent config; Kafka streams the configured crime CSV as simulated realtime data.

```bash
make run-all-15pct
```

Open:

```text
Dashboard: http://localhost:8501
Spark UI:  http://localhost:8081
Storm UI:  http://localhost:8088
```

## 3. Full Integrated Run

Use this when you are ready to process the full Spark datasets.

```bash
make run-all
```

## 4. Component Tests

Spark-only fast test:

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

Equivalent direct scripts:

```bash
./scripts/run_spark_batch.sh /app/config/config.spark_15pct.yaml
./scripts/run_spark_batch.sh /app/config/config.yaml
./scripts/run_storm_topology.sh
./scripts/run_kafka_producer.sh
./scripts/run_dashboard.sh
```

## 5. Logs

```bash
make logs-spark
make logs-producer
make logs-storm
make logs-dashboard
```

Spark also writes run logs to:

```text
logs/spark/
```

## 6. Spark Verification

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

Expected result: all six batch analytics tables should have rows for the latest completed `run_id`.

## 7. Streaming Verification

Storm topology:

```bash
docker exec storm-nimbus storm list
```

Kafka producer logs:

```bash
docker logs --tail 40 kafka-producer
```

Kafka topic check:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic crime-events --from-beginning --max-messages 5
```

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

Expected result: realtime district counts should update after Storm receives events. Alerts appear only when the configured threshold is crossed.

## 8. Dashboard Verification

```bash
curl -I http://localhost:8501
```

Expected:

```text
HTTP/1.1 200 OK
```

The dashboard should show:

```text
Batch job status
Crime Trends
Arrest Rates
Violence Analysis
Sex Offender Density
Hotspot Map
Correlations
Realtime District Counts
Realtime Alerts
```

If Kafka or Storm are not running, realtime sections may be empty. If Spark has not completed, historical sections should show a loading or empty-state message instead of crashing.

## 9. Database Utilities

PostgreSQL health:

```bash
docker exec postgres pg_isready -U crime_user -d crime_analytics
```

List PostgreSQL tables:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -Atc "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;"
```

Apply PostgreSQL schema manually:

```bash
docker exec -i postgres psql -U crime_user -d crime_analytics < db/postgres_init.sql
```

Recreate MongoDB indexes:

```bash
make setup-mongo
```

## 10. Configuration Checks

Full Spark config:

```text
config/config.yaml
```

Fast Spark test config:

```text
config/config.spark_15pct.yaml
```

Local host paths:

```bash
cp .env.example .env
```

Then edit if needed:

```dotenv
CRIME_ANALYTICS_PROJECT_ROOT=.
CRIME_ANALYTICS_DATA_DIR=./data
```

Use WSL/Linux paths for absolute values. Do not use Windows `C:\...` paths in Compose volumes.

## 11. Cleanup

Stop containers and keep database volumes:

```bash
make down
```

Delete containers and database volumes:

```bash
docker compose down -v
```

`-v` deletes PostgreSQL and MongoDB stored results.
