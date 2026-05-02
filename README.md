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

`district` is always treated as `STRING`. Numeric districts are normalized to zero-padded strings such as `001` and missing district values become `UNKNOWN`.

For sex offender density, the source dataset does not provide police district. Spark derives an approximate district by matching offender blocks to crime blocks. Blocks that cannot be matched, including missing block values, are kept as an `UNMATCHED` summary row and are not ranked as a real district.

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

Default Spark batch mode is the full dataset run:

```yaml
data:
  sample_mode: false
spark:
  progress_interval_seconds: 30
  log_dir: "/app/logs/spark"
```

For faster local Spark testing, use:

```text
config/config.spark_15pct.yaml
```

## Docker Startup

From WSL in the repository root:

```bash
docker compose up --build
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

Spark-only full dataset run:

```bash
make spark-up
make spark-run
make run-dashboard
```

Spark-only 15 percent test run:

```bash
make spark-up
make spark-run-15pct
make run-dashboard
```

Spark progress logs are written to:

```text
logs/spark/<run_id>.log
```

Watch the newest Spark run log:

```bash
tail -f "$(ls -t logs/spark/*.log | head -1)"
```

The dashboard violence section shows the full historical monthly range from the latest completed Spark run, including homicide victimizations back to 1991 when present in `violence.csv`. Detail tables are summarized so older records are not hidden by a short `LIMIT`.

Dashboard historical views include:

```text
Crime Trends: year, month, day_of_week, and hour charts
Arrest Rate Analysis: top 10 crime types by arrest rate plus primary_type/district/race detail
Violence Analysis: monthly totals, month-by-district detail, gunshot proportion, and top community areas
Sex Offender Density: ranked matched districts plus separate UNMATCHED summary
Hotspot Map: K-Means cluster centroids and crime counts
Correlations: violence-rate/arrest-rate and sex-offender-density/crime-rate rows
```

## Spark Batch Layer

Implemented:

```text
explicit Spark schemas for all five datasets
shared config loader
all five dataset loaders with sample/full config modes
cleaning and standardized fields
district string enforcement
crime trend analytics
arrest rate analysis
violence and gunshot analysis
sex offender density analysis with unmatched-block handling
K-Means hotspot detection
cross-dataset correlations
PostgreSQL staging table writes
staging-to-final publish after successful completion
batch_job_status running/completed/failed updates
```

Required Spark outputs:

```text
crime_trends
arrest_rates
violence_stats
sex_offender_density
hotspots
correlations
```

Coverage checks from the latest completed full run should show:

```text
crime_trends yearly total = crime CSV data rows
violence_stats monthly total_incidents = violence CSV data rows
sex_offender_density matched + UNMATCHED offender_count = sex offender CSV data rows
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
