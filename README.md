# Real-Time Crime Analytics and Intelligent Alert System

University Big Data Engineering project using Chicago public safety datasets with Lambda Architecture.

## Architecture

Option B is used: Spark batch processing and Kafka + Storm streaming run simultaneously.

Batch layer:

```text
CSV datasets -> PySpark batch jobs -> PostgreSQL analytical tables -> Streamlit historical views
```

Speed layer:

```text
Crime CSV replay -> Kafka crime-events topic -> Apache Storm topology -> PostgreSQL alerts + MongoDB alert_logs -> Streamlit live views
```

Spark answers historical questions. Kafka + Storm answer real-time questions. The dashboard combines both without requiring Spark to finish before live alerts can appear.

## Data Contract

Kafka messages and cleaned Spark DataFrames use standardized field names:

```text
case_number
date
block
primary_type
district
arrest
latitude
longitude
```

`district` is always treated as `STRING`. Missing district values become `UNKNOWN`.

## Expected Datasets

Place these files in `data/`:

```text
data/crimes.csv
data/arrests.csv
data/violence.csv
data/sex_offenders.csv
data/police_stations.csv
```

The current repository also supports the downloaded City of Chicago filenames dated `20260501`. Local symlinks can point the normalized names to those files.

## Configuration

All runtime values live in:

```text
config/config.yaml
```

Change dataset sampling, Kafka topic, producer speed, Storm threshold, database credentials, Spark settings, and dashboard refresh there only.

## Docker Startup

From WSL in the repository root:

```bash
docker-compose up --build
```

Required services:

```text
zookeeper
kafka
storm-nimbus
storm-supervisor
storm-ui
spark-master
spark-worker
postgres
mongodb
streamlit
```

Useful URLs:

```text
Streamlit: http://localhost:8501
Storm UI:  http://localhost:8088
Spark UI:  http://localhost:8081
Kafka:     localhost:9092
Postgres:  localhost:5432
MongoDB:   localhost:27017
```

## Execution

After containers are running:

```bash
make setup-mongo
make run-spark
make run-producer
make run-dashboard
```

Storm topology submission is reserved for the streaming implementation artifact:

```bash
make run-storm
```

## Current Batch Milestone

Implemented:

```text
explicit Spark schemas for all five datasets
shared config loader
crime CSV loading with sample mode
crime cleaning and standardized fields
district string enforcement
crime trend analytics
PostgreSQL staging table write
staging-to-final publish
batch_job_status running/completed/failed updates
```

First successful Spark milestone:

```text
Spark reads crimes.csv with explicit schema
district is cast to string
crime trends are computed
crime_trends_temp is populated
crime_trends is populated after success
batch_job_status changes from running to completed
```

## Latest Completed Batch Rule

Dashboard historical views use:

```sql
SELECT run_id
FROM batch_job_status
WHERE status = 'completed'
ORDER BY finished_at DESC
LIMIT 1;
```

If no completed run exists, the dashboard shows:

```text
Historical analytics are still being generated. Live alerts are active.
```

## Ownership Boundaries

Spark-owned tables:

```text
crime_trends
arrest_rates
violence_stats
sex_offender_density
hotspots
correlations
batch_job_status
```

Storm-owned tables:

```text
alerts
realtime_district_counts
```

Storm-owned MongoDB collections:

```text
raw_events
alert_logs
```

Spark must not consume Kafka or write real-time alert tables. Storm must not perform historical batch analytics.

## Development Branches

Recommended workflow:

```text
main
dev
spark-layer
streaming-layer
dashboard-layer
```

Coordinate changes to:

```text
config/config.yaml
db/postgres_init.sql
docker/docker-compose.yml
README.md
Makefile
```
