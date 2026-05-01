# Instructions

Project path:

```text
/home/ayaan/University/Big_Data_Analytics/Project/Real-time-crime-analytics
```

This document explains how to set up, run, test, and verify the Real-Time Crime Analytics and Intelligent Alert System.

## 1. Prerequisites

Required:

- Windows with WSL2 Ubuntu.
- Docker Desktop installed on Windows.
- Docker Desktop WSL integration enabled for Ubuntu.
- Git.
- VS Code is optional.

You do not need to manually install Kafka, Spark, Storm, PostgreSQL, or MongoDB on Windows. These services are provided through Docker Compose.

Verify Docker from inside WSL:

```bash
docker --version
docker compose version
docker ps
```

If these commands fail inside WSL, open Docker Desktop, enable WSL integration for Ubuntu, then restart the WSL terminal.

## 2. Project Setup

Go to the project root:

```bash
cd /home/ayaan/University/Big_Data_Analytics/Project/Real-time-crime-analytics
```

Check that Docker Compose can parse the project:

```bash
docker compose -f docker-compose.yml config
docker compose -f docker-compose.yml config --services
```

Expected important services include:

```text
zookeeper
kafka
postgres
mongodb
spark-master
spark-worker
storm-nimbus
storm-supervisor
storm-ui
streamlit
kafka-producer
mongo-setup
```

### Command Styles

This project supports two equivalent command styles:

1. Make targets for shorter day-to-day commands.
2. Direct shell scripts for explicit component-level testing and debugging.

The Make targets are wrappers around the scripts where applicable.

```text
make spark-run / make run-spark             -> ./scripts/run_spark_batch.sh
make spark-run-15pct / make run-spark-15pct -> ./scripts/run_spark_batch.sh /app/config/config.spark_15pct.yaml
make run-storm                              -> ./scripts/run_storm_topology.sh
make run-producer                           -> ./scripts/run_kafka_producer.sh
make run-dashboard                          -> ./scripts/run_dashboard.sh
make setup-mongo                            -> ./scripts/setup_mongo.sh
```

Use Make commands for normal testing. Use the scripts directly when you want to pass arguments or debug one component.

### Spark Config Profiles

Spark batch runs are controlled by config files. Do not hardcode dataset sizes inside Spark code.

Default Spark config:

```text
/app/config/config.yaml
```

15 percent local test config:

```text
/app/config/config.spark_15pct.yaml
```

The 15 percent test mode is configured in:

```text
config/config.spark_15pct.yaml
```

Relevant keys:

```yaml
data:
  sample_mode: true
  crime_sample_rows: 1281247
  other_sample_rows: 10000
```

To change the local test size, edit those values in `config/config.spark_15pct.yaml`, then run Spark with the named 15 percent target. For a new test profile, copy the config file and pass the new path to either the Make target or the script.

## 3. Full Project Run

This is the final expected project flow for the full Lambda Architecture system.

### Option A: Make Targets

Start all core services with Make:

```bash
make up
```

Note: `make up` runs Docker Compose in the foreground so logs stay visible. For a detached run, use the Docker Compose command shown in Option B.

Run the streaming topology:

```bash
make run-storm
```

Start the Kafka crime-event producer:

```bash
make run-producer
```

Run the Spark batch analytics:

```bash
make run-spark
```

Start or verify the dashboard:

```bash
make run-dashboard
```

### Option B: Direct Compose and Scripts

Start all core services in the background:

```bash
docker compose up -d --build
```

Run the streaming topology:

```bash
./scripts/run_storm_topology.sh
```

Start the Kafka crime-event producer:

```bash
./scripts/run_kafka_producer.sh
```

Run the Spark batch analytics:

```bash
./scripts/run_spark_batch.sh
```

Start or verify the dashboard:

```bash
./scripts/run_dashboard.sh
```

Open the dashboard:

```text
http://localhost:8501
```

Useful service UIs and ports:

```text
Dashboard:  http://localhost:8501
Spark UI:   http://localhost:8081
Storm UI:   http://localhost:8088
Kafka:      localhost:9092
PostgreSQL: localhost:5432
MongoDB:    localhost:27017
```

Expected behavior:

- Kafka streams crime records row by row.
- Storm processes live events and writes real-time alerts.
- Spark processes historical CSV data and writes batch analytics.
- Streamlit shows live alerts immediately and historical analytics after a completed Spark run exists.

## 4. Spark-Only Run: Default Config

Use this when testing only the Spark batch layer and dashboard.

### Option A: Make Targets

```bash
make spark-up
make spark-run
make run-dashboard
```

### Option B: Direct Compose and Scripts

```bash
docker compose -f docker-compose.yml up -d --build postgres spark-master spark-worker
docker compose -f docker-compose.yml up -d --build --no-deps streamlit
./scripts/run_spark_batch.sh
./scripts/run_dashboard.sh
```

This runs Spark using:

```text
/app/config/config.yaml
```

The default config uses the normal configured sample/full-data settings in `config/config.yaml`.

## 5. Spark-Only Run: 15 Percent Test Data

This is the recommended local test mode because it is faster than processing the full dataset.

The test size is configured in:

```text
config/config.spark_15pct.yaml
```

Do not edit Spark code to change the test size. Update the config values instead:

```yaml
data:
  sample_mode: true
  crime_sample_rows: 1281247
  other_sample_rows: 10000
```

### Option A: Make Targets

```bash
make spark-up
make spark-run-15pct
make run-dashboard
```

For a custom Spark config file, use:

```bash
make spark-run SPARK_CONFIG=/app/config/your_config.yaml
```

### Option B: Direct Compose and Scripts

```bash
docker compose -f docker-compose.yml up -d --build postgres spark-master spark-worker
docker compose -f docker-compose.yml up -d --build --no-deps streamlit
./scripts/run_spark_batch.sh /app/config/config.spark_15pct.yaml
./scripts/run_dashboard.sh
```

Open:

```text
http://localhost:8501
```

## 6. Spark Verification Queries

Check recent Spark batch job status:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -c "
SELECT run_id, job_name, status, started_at, finished_at, message
FROM batch_job_status
ORDER BY created_at DESC
LIMIT 5;
"
```

Verify that all Spark batch output tables have rows for the latest completed run:

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

Expected result:

- `batch_job_status` should show a latest `completed` run.
- All six Spark analytics tables should have counts greater than `0`.

## 7. Dashboard Verification

Check that Streamlit is responding:

```bash
curl -I http://localhost:8501
```

Expected:

```text
HTTP/1.1 200 OK
```

The dashboard should show:

- Crime Trends
- Arrest Rates
- Violence Analysis
- Sex Offender Density
- Hotspot Map
- Correlations
- Batch job status

If Kafka and Storm are not running, real-time alerts may be empty. That is expected during Spark-only testing.

## 8. Kafka + Storm Component Testing

These commands are for the streaming teammate and integration phase.

### Option A: Make Targets

Start the Storm topology:

```bash
make run-storm
```

Start the Kafka crime-event producer:

```bash
make run-producer
```

### Option B: Direct Scripts

Start the Storm topology:

```bash
./scripts/run_storm_topology.sh
```

Start the Kafka crime-event producer:

```bash
./scripts/run_kafka_producer.sh
```

Useful checks:

```bash
docker compose -f docker-compose.yml ps storm-nimbus storm-supervisor storm-ui kafka zookeeper
docker logs storm-nimbus --tail 100
docker logs storm-supervisor --tail 100
docker logs kafka-producer --tail 100
```

Streaming output should eventually appear in:

```text
PostgreSQL table: alerts
PostgreSQL table: realtime_district_counts
MongoDB collection: alert_logs
```

Kafka and Storm should not duplicate Spark analytics. Spark handles historical batch analytics; Kafka and Storm handle simulated live crime events and alerts.

## 9. Database Utilities

Check PostgreSQL health:

```bash
docker exec postgres pg_isready -U crime_user -d crime_analytics
```

Check created tables:

```bash
docker exec postgres psql -U crime_user -d crime_analytics -Atc "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;"
```

Apply the PostgreSQL schema manually if needed:

```bash
docker exec -i postgres psql -U crime_user -d crime_analytics < db/postgres_init.sql
```

Check MongoDB health:

```bash
docker exec mongodb mongosh --quiet --eval "db.adminCommand('ping')"
```

Run MongoDB index/setup script if needed:

Make target:

```bash
make setup-mongo
```

Direct script:

```bash
./scripts/setup_mongo.sh
```

## 10. Stopping and Cleaning

Stop containers but keep database volumes:

Make target:

```bash
make down
```

Direct Docker Compose command:

```bash
docker compose down
```

Full cleanup:

```bash
docker compose down -v
```

Warning: `docker compose down -v` deletes Docker volumes, including PostgreSQL and MongoDB stored results.

## 11. Git Workflow

Create and push the Spark branch:

```bash
git checkout -b ayaan-spark-branch
git add .
git commit -m "Complete Spark batch analytics layer"
git push -u origin ayaan-spark-branch
```

Open a pull request from:

```text
ayaan-spark-branch -> main
```

Before committing, it is useful to check:

```bash
git status
git diff --stat
```

## 12. Notes

- Spark uses explicit schemas only.
- `inferSchema=True` must not be used.
- `district` is standardized as string.
- Batch outputs use `run_id`.
- Spark writes to temp tables first, then publishes to final tables only after successful completion.
- Dashboard reads the latest completed Spark run only.
- Failed or running Spark runs are ignored by the dashboard for historical analytics.
- Kafka and Storm integration is separate from Spark and should not duplicate Spark analytics.
- Spark-owned tables are `crime_trends`, `arrest_rates`, `violence_stats`, `sex_offender_density`, `hotspots`, `correlations`, and `batch_job_status`.
- Storm-owned tables are `alerts` and `realtime_district_counts`.
