# Real-Time Crime Analytics and Intelligent Alert System

**Contributors:** 
- Ayaan Khan
- Minahil Ali

A Big Data Engineering project that analyzes Chicago public safety datasets using a Lambda Architecture. The system combines historical batch analytics with simulated real-time crime event streaming, anomaly detection, persistent storage, and a Streamlit dashboard.

## Overview

The project answers two different public safety analytics questions at the same time:

- **Historical analytics:** What has happened over time?
- **Real-time alerts:** What is happening right now?

```text
Historical CSV datasets
  -> Apache Spark / PySpark
  -> PostgreSQL analytical tables
  -> Streamlit historical dashboard views
```

```text
Crime CSV replay
  -> Kafka producer
  -> Kafka topic: crime-events
  -> Apache Storm topology
  -> PostgreSQL realtime tables + MongoDB alert logs
  -> Streamlit live dashboard views
```

Spark owns the batch layer. Kafka and Storm own the speed layer. The dashboard only reads from PostgreSQL and MongoDB, so it can show live alerts even while Spark batch jobs are still running.

## Key Features

- Dockerized local big-data stack using Docker Compose.
- Spark batch processing with explicit `StructType` schemas.
- Kafka producer that replays crime records as JSON events.
- Apache Storm topology for sliding-window anomaly detection.
- PostgreSQL serving layer for structured analytics and alerts.
- MongoDB serving layer for alert log documents.
- Streamlit dashboard for historical analytics and real-time monitoring.
- Config-driven paths, sample sizes, thresholds, and service settings.
- Staging-to-final publish pattern for Spark outputs to avoid partial results.

## Tech Stack

| Layer | Technology |
| --- | --- |
| Batch processing | Apache Spark, PySpark |
| Streaming ingestion | Apache Kafka |
| Stream processing | Apache Storm |
| Structured storage | PostgreSQL |
| Document storage | MongoDB |
| Dashboard | Streamlit |
| Orchestration | Docker Compose, Makefile |

## Batch Analytics Implemented

Spark computes and writes the following analytics to PostgreSQL:

- Crime trends by year, month, day of week, and hour.
- Arrest rates by crime type, district, and race.
- Violence and gunshot statistics by month, district, and community area.
- Sex offender density by derived district where possible.
- Geospatial hotspots using K-Means clustering.
- Cross-dataset correlations such as violence rate vs arrest rate.

All Spark outputs include a deterministic `run_id`, write to `_temp` staging tables first, and publish to final tables only after the batch run completes successfully.

## Streaming Analytics Implemented

Kafka streams rows from the crime dataset into the `crime-events` topic. Storm consumes those events through this topology:

```text
KafkaSpout -> ParseBolt -> DistrictBolt -> WindowBolt -> AnomalyBolt -> AlertBolt
```

The topology counts events per district inside a sliding window and generates alerts when counts exceed configurable thresholds.

Default severity rules:

```text
25 to 39 events = LOW
40 to 59 events = MEDIUM
60+ events     = HIGH
```

## Repository Structure

```text
.
├── config/              # Runtime configuration
├── dashboard/           # Streamlit dashboard
├── data/                # Local CSV datasets
├── db/                  # PostgreSQL schema and MongoDB setup
├── docker/              # Docker compatibility wrapper
├── kafka/               # Kafka producer
├── scripts/             # Run scripts
├── spark/               # PySpark schemas, preprocessing, analytics, ML
├── storm/               # Storm topology, spouts, and bolts
├── docker-compose.yml   # Main Docker Compose file
├── Makefile             # Main run commands
├── README.md
├── REPORT.md
└── TESTING.md
```

## Prerequisites

Recommended environment:

- Windows with WSL2 Ubuntu.
- Docker Desktop installed on Windows.
- Docker Desktop WSL integration enabled.
- Git.
- Make.

No manual Windows installation is required for Spark, Kafka, Storm, PostgreSQL, or MongoDB. Those services run in Docker containers.

Verify Docker from WSL:

```bash
docker --version
docker compose version
docker ps
```

## Dataset Setup

Place the five Chicago public safety CSV files in `data/`:

```text
data/crimes.csv
data/arrests.csv
data/violence.csv
data/sex_offenders.csv
data/police_stations.csv
```

The loader also supports the downloaded City of Chicago filenames, for example:

```text
Crimes_-_2001_to_Present_20260501.csv
Arrests_20260501.csv
Violence_Reduction_-_Victims_of_Homicides_and_Non-Fatal_Shootings_20260501.csv
Sex_Offenders_20260501.csv
Police_Stations_20260501.csv
```

Large datasets are not committed to the repository. Download them from the City of Chicago Open Data Portal and place them in the local `data/` directory.

## Local Path Configuration

The root `docker-compose.yml` is the main Compose file. The file at `docker/docker-compose.yml` only includes the root file for backwards compatibility.

If your repo or data directory differs from the defaults, create a local `.env` file:

```bash
cp .env.example .env
```

Then edit:

```dotenv
CRIME_ANALYTICS_PROJECT_ROOT=.
CRIME_ANALYTICS_DATA_DIR=./data
```

Path rules:

- Use WSL/Linux paths, not Windows `C:\...` paths.
- Do not hardcode personal paths in `docker-compose.yml`, `Makefile`, or scripts.
- Inside containers, the project is mounted at `/app`.
- Inside containers, datasets are mounted at `/app/data`.
- Keep `config/config.yaml` using `data.base_path: "/app/data"`.

## Configuration

Main full-run config:

```text
config/config.yaml
```

Fast Spark test config:

```text
config/config.spark_15pct.yaml
```

Important settings:

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

`config/config.yaml` is the default full run. `config/config.spark_15pct.yaml` limits large Spark inputs to about 15 percent for faster local testing. Kafka still streams the configured crime dataset as simulated live data.

To slow down simulated real-time replay, edit:

```yaml
kafka:
  producer_interval_seconds: 15
```

## Quick Start

From the repository root:

```bash
cd /home/ayaan/University/Big_Data_Analytics/Project/Real-time-crime-analytics
```

For another machine, replace that path with the local WSL repo path.

Run the fast integrated test:

```bash
make run-all-15pct
```

Run the full project:

```bash
make run-all
```

These targets start infrastructure, MongoDB indexes, Storm topology, Kafka producer, Spark batch processing, and the Streamlit dashboard.

Open:

```text
Dashboard: http://localhost:8501
Spark UI:  http://localhost:8081
Storm UI:  http://localhost:8088
```

## Component Commands

Start shared services:

```bash
make up-detached
```

Run Spark only with the 15 percent test config:

```bash
make spark-up
make spark-run-15pct
make run-dashboard
```

Run Spark only with the full config:

```bash
make spark-up
make spark-run
make run-dashboard
```

Run Kafka and Storm only:

```bash
make up-detached
make setup-mongo
make run-storm
make run-producer
make run-dashboard
```

Direct scripts are also available, but the Makefile targets are preferred for normal runs:

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

Spark run logs are written to:

```text
logs/spark/
```

## Stop and Cleanup

Stop containers while keeping database volumes:

```bash
make down
```

Delete containers and database volumes:

```bash
docker compose down -v
```

`-v` deletes PostgreSQL and MongoDB stored results.

## Implementation Notes

- Spark uses explicit `StructType` schemas only.
- `inferSchema=True` is not used.
- `district` is standardized as a string across Spark, Kafka, Storm, PostgreSQL, and MongoDB.
- Spark writes temporary staging tables first, then publishes final tables after success.
- Dashboard reads only the latest completed Spark `run_id`.
- Kafka and Storm do not run historical analytics.
- Raw MongoDB event logging is disabled by default to avoid uncontrolled storage growth.
- The sex offender dataset does not provide a police district field, so district is derived where possible using matched crime-block evidence. Unmatched records are retained separately and are not ranked as a real district.


## Dataset Sources

- Crime Data (2001-Present): [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data)
- Police Stations: [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Police-Stations/z8bn-74gv/about_data)
- Arrests: [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Arrests/dpt3-jri9/about_data)
- Violence Reduction (Homicides and Non-Fatal Shootings): [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Violence-Reduction-Victims-of-Homicides-and-Non-Fa/gumc-mgzr/about_data)
- Sex Offenders: [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Sex-Offenders/vc9r-bqvy/about_data)
