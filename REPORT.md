# Real-Time Crime Analytics and Intelligent Alert System

## 1. Architecture

The project implements Lambda Architecture with two active data paths.

```text
Historical CSV datasets
  -> PySpark batch jobs
  -> PostgreSQL analytical tables
  -> Streamlit historical dashboard views
```

```text
Crime CSV replay
  -> Kafka producer
  -> Kafka topic: crime-events
  -> Apache Storm topology
  -> PostgreSQL realtime tables + MongoDB alert_logs
  -> Streamlit live dashboard views
```

Spark and Storm run simultaneously. Spark answers historical questions. Kafka + Storm answer realtime questions. The dashboard reads results from the serving layer only.

## 2. Components

Batch layer:

- PySpark.
- Explicit `StructType` schemas for all five datasets.
- Standardized snake_case field names.
- `district` normalized as a zero-padded string.
- One `run_id` per full batch run.
- Staging-to-final PostgreSQL publish strategy.

Speed layer:

- Kafka producer replays `crimes.csv` row by row.
- Kafka topic is `crime-events`.
- Storm topology is `KafkaSpout -> ParseBolt -> DistrictBolt -> WindowBolt -> AnomalyBolt -> AlertBolt`.
- Windowing uses processing time so replayed historical rows behave like live arrivals.

Serving layer:

- PostgreSQL stores batch analytics, realtime district counts, and alerts.
- MongoDB stores `alert_logs`; raw event logging is disabled by default.
- Streamlit dashboard reads latest completed Spark run plus realtime tables.

## 3. Data Contract

Kafka event fields:

```json
{
  "case_number": "...",
  "date": "...",
  "block": "...",
  "primary_type": "...",
  "district": "...",
  "arrest": true,
  "latitude": 41.0,
  "longitude": -87.0
}
```

Rules:

- `district` is always a string.
- Numeric districts are zero-padded, for example `001`.
- Missing districts become `UNKNOWN`.
- Shared field names stay snake_case.

## 4. Configuration and Paths

Runtime config:

```text
config/config.yaml
```

15 percent Spark test config:

```text
config/config.spark_15pct.yaml
```

Machine-specific host paths:

```bash
cp .env.example .env
```

Then edit:

```dotenv
CRIME_ANALYTICS_PROJECT_ROOT=.
CRIME_ANALYTICS_DATA_DIR=./data
```

Rules:

- Use WSL/Linux paths for absolute values.
- Do not use Windows `C:\...` paths in Compose volumes.
- Do not hardcode personal paths in `docker-compose.yml`, `Makefile`, or scripts.
- Containers use `/app` for project code and `/app/data` for CSVs.

Required CSV filenames:

```text
data/crimes.csv
data/arrests.csv
data/violence.csv
data/sex_offenders.csv
data/police_stations.csv
```

Downloaded City of Chicago filenames are also supported as fallbacks.

## 5. Batch Analytics

Spark computes:

- Crime trends by year, month, day of week, and hour.
- Arrest rates by primary crime type, district, and race.
- Violence and gunshot statistics by month, district, and community area.
- Sex offender density with matched police district names where available.
- K-Means hotspot centroids and cluster counts.
- Cross-dataset correlations.

Sex offender limitation:

- The sex offender CSV does not directly include a police district field.
- The Spark job derives district where possible using matched crime-block evidence.
- Unmatched records are retained for transparency but are not treated as a named police district in ranked dashboard views.

Spark-owned PostgreSQL tables:

```text
crime_trends
arrest_rates
violence_stats
sex_offender_density
hotspots
correlations
batch_job_status
```

Spark writes `_temp` tables first. Final tables are published only after all analytics succeed.

## 6. Streaming Analytics

The Kafka producer streams all configured crime rows by default.

Current default:

```yaml
kafka:
  producer_interval_seconds: 1
```

That means one event every second. To simulate slower live arrival, set `producer_interval_seconds` to `15` or `30`.

Storm windows:

```yaml
storm:
  window_size_seconds: 300
  slide_interval_seconds: 60
  anomaly_threshold: 25
```

Severity:

```text
25 to 39 = LOW
40 to 59 = MEDIUM
60+ = HIGH
```

Storm-owned PostgreSQL tables:

```text
alerts
realtime_district_counts
```

Storm-owned MongoDB collection:

```text
alert_logs
```

## 7. Run Commands

Run from the repository root in WSL.

Fast integrated test:

```bash
make run-all-15pct
```

Full project run:

```bash
make run-all
```

Spark only:

```bash
make spark-up
make spark-run-15pct
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

Equivalent scripts:

```bash
./scripts/run_spark_batch.sh /app/config/config.yaml
./scripts/run_spark_batch.sh /app/config/config.spark_15pct.yaml
./scripts/run_storm_topology.sh
./scripts/run_kafka_producer.sh
./scripts/run_dashboard.sh
```

Open:

```text
Dashboard: http://localhost:8501
Spark UI:  http://localhost:8081
Storm UI:  http://localhost:8088
```

## 8. Log Commands

```bash
make logs-spark
make logs-producer
make logs-storm
make logs-dashboard
```

Spark run files:

```text
logs/spark/
```

## 9. Full Verification Commands

Validate Compose:

```bash
docker compose -f docker-compose.yml --profile streaming --profile java-submit --profile harness config
docker compose -f docker-compose.yml --profile streaming --profile java-submit --profile harness config --services
```

Check running services:

```bash
docker compose -f docker-compose.yml --profile streaming --profile java-submit ps -a
```

Check PostgreSQL health:

```bash
docker exec postgres pg_isready -U crime_user -d crime_analytics
```

Check MongoDB health:

```bash
docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping')"
```

Check Storm topology:

```bash
docker exec storm-nimbus storm list
```

Check dashboard:

```bash
curl -I http://localhost:8501
```

## 10. Spark Verification

Latest batch status:

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

Check trend types:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -c "
WITH latest AS (
  SELECT run_id
  FROM batch_job_status
  WHERE status = 'completed'
  ORDER BY finished_at DESC
  LIMIT 1
)
SELECT trend_type, COUNT(*) AS rows, SUM(crime_count) AS total_count
FROM crime_trends
WHERE run_id = (SELECT run_id FROM latest)
GROUP BY trend_type
ORDER BY trend_type;
"
```

## 11. Streaming Verification

Kafka producer status:

```bash
docker logs --tail 40 kafka-producer
```

Kafka topic:

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
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

MongoDB counts:

```bash
docker exec mongodb mongosh crime_analytics --quiet --eval "printjson({alert_logs: db.alert_logs.countDocuments(), raw_events: db.raw_events.countDocuments()})"
```

## 12. Expected Results

After `make run-all-15pct`:

- Storm topology should be `ACTIVE`.
- Kafka producer should stream from `/app/data/crimes.csv`.
- Spark latest `batch_job_status` should be `completed`.
- All six Spark output tables should have rows for the latest completed `run_id`.
- `realtime_district_counts` should update first.
- `alerts` and MongoDB `alert_logs` update when thresholds are crossed.
- Dashboard should load at `http://localhost:8501`.

Normal behavior:

- `alerts` can be empty early in a run if no district has crossed threshold.
- `raw_events` remains empty unless raw event logging is enabled.
- Dashboard historical sections use only the latest completed Spark run.

## 13. Reset Commands

Stop containers but keep database volumes:

```bash
make down
```

Clean all stored database results:

```bash
docker compose down -v
```

Reapply PostgreSQL schema manually:

```bash
docker exec -i postgres psql -U crime_user -d crime_analytics < db/postgres_init.sql
```

Recreate MongoDB indexes:

```bash
make setup-mongo
```

Redeploy Storm topology:

```bash
make run-storm
```

## 14. Troubleshooting

Storm leader errors:

- Run `make run-storm`.
- The script checks Nimbus health and recreates stale Zookeeper/Kafka/Storm containers if leader election is broken.

Dashboard shows no historical data:

- Check `batch_job_status`.
- The dashboard ignores `running` and `failed` Spark runs.

Realtime counts are empty:

- Check `docker logs --tail 40 kafka-producer`.
- Check `docker exec storm-nimbus storm list`.
- Wait at least one Storm slide interval, currently 60 seconds.

No alerts:

- Check `realtime_district_counts` first.
- Alerts require `event_count >= 25` by default.

Dataset path errors:

- Check `.env`.
- Check `CRIME_ANALYTICS_DATA_DIR`.
- Check `config/config.yaml` still uses `/app/data`.

## 15. Last Verified State

The integrated 15 percent test was run with:

```bash
make run-all-15pct
```

Verified outputs:

```text
Spark batch status: completed
Storm topology: ACTIVE
Dashboard: HTTP 200
PostgreSQL realtime_district_counts: updating
PostgreSQL alerts: populated when thresholds are crossed
MongoDB alert_logs: populated
```
