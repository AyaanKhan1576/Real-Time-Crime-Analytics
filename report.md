# Real-Time Crime Analytics and Intelligent Alert System - Person 2 Report

## Scope

This report documents the Kafka + Storm speed layer for the Lambda Architecture project. Person 2 owns:

- Kafka CSV replay producer
- Storm topology: KafkaSpout -> ParseBolt -> DistrictBolt -> WindowBolt -> AnomalyBolt -> AlertBolt
- Real-time PostgreSQL writes to `alerts` and `realtime_district_counts`
- MongoDB writes to `alert_logs`
- Optional TTL-controlled `raw_events`
- Streaming validation and demo commands

Spark batch analytics are intentionally separate and must not write to the speed-layer tables.

## Current Architecture

```text
Crime CSV rows
  -> Kafka producer
  -> Kafka topic: crime-events
  -> Java Storm topology
  -> ParseBolt
  -> DistrictBolt
  -> WindowBolt
  -> AnomalyBolt
  -> AlertBolt
  -> PostgreSQL: realtime_district_counts, alerts
  -> MongoDB: alert_logs
```

## Recent Fixes

### 1. Strict Java ParseBolt validation

`java-topology/src/main/java/com/project/bolts/ParseBolt.java` now validates the full Kafka message contract before emitting:

- `case_number`
- `date`
- `block`
- `primary_type`
- `district`
- `arrest`
- `latitude`
- `longitude`

Malformed messages are logged and dropped. `district` is still normalized to string, and missing/blank district becomes `UNKNOWN`.

### 2. Java topology reads `config/config.yaml`

`java-topology/src/main/java/com/project/TopologyConfig.java` now loads values from `config/config.yaml` through SnakeYAML. Environment variables still override YAML values when needed.

This prevents the submitted Java topology from drifting away from the shared project config.

### 3. Mongo indexes initialize automatically

`docker/docker-compose.yml` now includes a one-shot `mongo-setup` service. It runs `db/mongo_setup.py`, creates the `raw_events` TTL index and the `alert_logs` indexes, then exits successfully.

Storm services that need Mongo now wait for `mongo-setup` to complete.

### 4. AlertBolt now acknowledges tuples

`java-topology/src/main/java/com/project/bolts/AlertBolt.java` now calls `collector.ack(tuple)` after processing.

Before this fix, `AlertBolt` could write rows to PostgreSQL but Storm UI still showed `acked = 0`. That was misleading because the sink was doing work but not telling Storm that input tuples were complete.

### 5. WindowBolt uses processing time for live simulation

`java-topology/src/main/java/com/project/bolts/WindowBolt.java` now windows events by processing time instead of the historical CSV `date` value.

This matters because the project simulates a live system by replaying old CSV rows. If the old `date` field is used for windowing, replayed rows may be far apart in event time and district counts can stay at `1`, preventing alerts even while Kafka and Storm are working. Processing-time windows make the replay behave like live incoming events.

## Why Some Storm UI Values Are Zero

### AlertBolt emitted = 0 and transferred = 0

This is expected.

`AlertBolt` is the final sink bolt. It writes to PostgreSQL and MongoDB. It does not emit tuples to another downstream bolt, so:

```text
alert-bolt emitted = 0
alert-bolt transferred = 0
```

That is normal for a sink.

### AlertBolt acked = 0

This was not ideal. The Java `AlertBolt` previously did not call `collector.ack(tuple)`.

That has now been fixed. After rebuilding and resubmitting the topology, `alert-bolt` ack counts should increase as it processes tuples.

### PostgreSQL alerts = 0 while realtime_district_counts > 0

This can be normal.

`realtime_district_counts` is written for every emitted district window count. `alerts` is written only when the count crosses the configured severity thresholds:

```yaml
storm:
  severity:
    low_min: 25
    medium_min: 40
    high_min: 60
```

If current windows contain fewer than 25 events per district, the system still writes live district counts but does not create alerts.

Your example:

```text
alerts                   0
realtime_district_counts 22
```

means Storm is processing windows and writing live counts, but no district window has reached the LOW alert threshold yet.

In an earlier run, the max district window count was `1` because the topology was using historical CSV event time. That has been changed to processing time for the Java topology so replayed rows accumulate in the active five-minute window.

### Mongo alert_logs = 0

This matches `alerts = 0`.

MongoDB `alert_logs` are written only for actual alerts. If no alert is created in PostgreSQL, no alert document is written to MongoDB.

### Mongo raw_events = 0

This is expected by default.

Raw event logging is disabled in `config/config.yaml`:

```yaml
mongodb:
  enable_raw_event_logging: false
```

This follows the project rule that raw MongoDB event logging must be disabled by default or TTL-controlled.

## Real-Time Demo Commands

Run these from the repository root:

```powershell
cd "D:\university\SEMESTER 8\big data\crime analytics"
```

Start the cluster:

```powershell
docker compose -f docker/docker-compose.yml up -d
```

Build the Java topology image after code changes:

```powershell
docker compose -f docker/docker-compose.yml --profile java-submit build storm-submit-java
```

Submit or resubmit the Storm topology:

```powershell
docker compose -f docker/docker-compose.yml --profile java-submit up -d --force-recreate storm-submit-java
```

Open Storm UI:

```text
http://localhost:8080
```

List active topologies:

```powershell
docker compose -f docker/docker-compose.yml exec -T storm-nimbus storm list
```

Run the producer in real-time simulation mode, one row per second:

```powershell
docker compose -f docker/docker-compose.yml run --rm `
  -e DATA__BASE_PATH=/app/data/raw `
  -e DATA__CRIME_FILE=Crimes_-_2001_to_Present_20260501.csv `
  -e KAFKA__PRODUCER_RATE_PER_SECOND=1 `
  storm-harness bash -lc "python kafka/producer.py --config config/config.yaml"
```

Run a faster demo replay:

```powershell
docker compose -f docker/docker-compose.yml run --rm `
  -e DATA__BASE_PATH=/app/data/raw `
  -e DATA__CRIME_FILE=Crimes_-_2001_to_Present_20260501.csv `
  -e KAFKA__PRODUCER_RATE_PER_SECOND=25 `
  storm-harness bash -lc "python kafka/producer.py --config config/config.yaml --max-rows 5000"
```

Run an unthrottled replay:

```powershell
docker compose -f docker/docker-compose.yml run --rm `
  -e DATA__BASE_PATH=/app/data/raw `
  -e DATA__CRIME_FILE=Crimes_-_2001_to_Present_20260501.csv `
  -e KAFKA__PRODUCER_RATE_PER_SECOND=0 `
  storm-harness bash -lc "python kafka/producer.py --config config/config.yaml"
```

## Demo Command To Force Alerts

For a live class demo, the normal threshold of 25 may take time to trigger depending on event timestamps and district distribution.

To prove alert creation quickly, temporarily lower the Java topology thresholds through environment variables at submit time:

```powershell
docker compose -f docker/docker-compose.yml exec -T storm-nimbus storm kill crime-alert-topology -w 1
```

```powershell
docker compose -f docker/docker-compose.yml run --rm `
  -e STORM_SEVERITY_LOW_MIN=1 `
  -e STORM_SEVERITY_MEDIUM_MIN=2 `
  -e STORM_SEVERITY_HIGH_MIN=3 `
  -e STORM_ANOMALY_THRESHOLD=1 `
  -e STORM_SLIDE_INTERVAL_SECONDS=5 `
  storm-submit-java
```

Then run a short producer replay:

```powershell
docker compose -f docker/docker-compose.yml run --rm `
  -e DATA__BASE_PATH=/app/data/raw `
  -e DATA__CRIME_FILE=Crimes_-_2001_to_Present_20260501.csv `
  -e KAFKA__PRODUCER_RATE_PER_SECOND=10 `
  storm-harness bash -lc "python kafka/producer.py --config config/config.yaml --max-rows 500"
```

After the demo, return to normal thresholds by killing and resubmitting without the threshold overrides:

```powershell
docker compose -f docker/docker-compose.yml exec -T storm-nimbus storm kill crime-alert-topology -w 1
docker compose -f docker/docker-compose.yml --profile java-submit up -d --force-recreate storm-submit-java
```

## Verification Commands

Check PostgreSQL row counts:

```powershell
docker exec docker-postgres-1 psql -U crime_user -d crime_analytics -c "select 'alerts' as table_name, count(*) from alerts union all select 'realtime_district_counts', count(*) from realtime_district_counts;"
```

Check recent district counts:

```powershell
docker exec docker-postgres-1 psql -U crime_user -d crime_analytics -c "select district, event_count, window_start, window_end from realtime_district_counts order by window_end desc limit 10;"
```

Check alert rows:

```powershell
docker exec docker-postgres-1 psql -U crime_user -d crime_analytics -c "select alert_id, district, event_count, severity, alert_timestamp from alerts order by alert_timestamp desc limit 10;"
```

Check MongoDB collections:

```powershell
docker exec docker-mongo-1 mongosh crime_analytics --quiet --eval "printjson({alert_logs: db.alert_logs.countDocuments(), raw_events: db.raw_events.countDocuments()})"
```

Check recent MongoDB alert logs:

```powershell
docker exec docker-mongo-1 mongosh crime_analytics --quiet --eval "db.alert_logs.find().sort({timestamp:-1}).limit(5).forEach(printjson)"
```

Check Storm submit logs:

```powershell
docker compose -f docker/docker-compose.yml logs --tail=120 storm-submit-java
```

Check Storm worker logs for errors:

```powershell
docker compose -f docker/docker-compose.yml exec -T storm-supervisor sh -c "grep -R -n -E 'ERROR|Exception|Caused by|ClassNotFound|NoClassDef|Failed' /apache-storm-2.6.0/logs/workers-artifacts || true"
```

## Reset Commands

Stop the cluster:

```powershell
docker compose -f docker/docker-compose.yml down
```

Stop and remove volumes for a clean database:

```powershell
docker compose -f docker/docker-compose.yml down -v
```

Kill the active topology:

```powershell
docker compose -f docker/docker-compose.yml exec -T storm-nimbus storm kill crime-alert-topology -w 1
```

Resubmit the topology:

```powershell
docker compose -f docker/docker-compose.yml --profile java-submit up -d --force-recreate storm-submit-java
```

## Expected Results

During normal real-time simulation:

- Storm UI should show the topology as `ACTIVE`.
- Kafka spout and middle bolts should emit and ack tuples.
- `alert-bolt` should ack tuples after the ack fix.
- `alert-bolt` should still show `emitted = 0` and `transferred = 0` because it is a sink.
- `realtime_district_counts` should increase before `alerts`.
- `alerts` and MongoDB `alert_logs` increase only when event counts cross severity thresholds.
- `raw_events` remains zero unless raw event logging is explicitly enabled.
- Java `WindowBolt` uses processing time, so replayed CSV rows are treated as live arrivals for sliding-window counts.

## Troubleshooting Notes

### Storm UI 500 with `NotAliveException`

This usually means the browser is viewing an old topology id. Go back to:

```text
http://localhost:8080
```

Hard refresh, then click the current `crime-alert-topology` row.

### Alerts stay at zero

This usually means the topology is processing but no district window reached `low_min`.

Check:

```powershell
docker exec docker-postgres-1 psql -U crime_user -d crime_analytics -c "select max(event_count), count(*) from realtime_district_counts;"
```

If `max(event_count)` is below 25, alerts staying at zero is correct.

For demo purposes, use the temporary threshold override commands above.

### raw_events stays at zero

This is correct unless `mongodb.enable_raw_event_logging` is set to `true`.

Keeping it disabled by default satisfies the project requirement.

## Current Person 2 Status

Completed:

- Kafka producer emits agreed JSON field names.
- Kafka topic is `crime-events`.
- Java Storm topology runs in Docker.
- Storm validates and drops malformed messages.
- `district` is string everywhere in the speed layer.
- Sliding window counts are written to PostgreSQL.
- Deterministic `alert_id` is used for alert deduplication.
- Alerts are written to PostgreSQL and MongoDB when thresholds are crossed.
- Mongo raw event logging is disabled by default and TTL-indexed when enabled.
- Mongo indexes are initialized automatically by Docker Compose.

Remaining demo task:

- Run the producer continuously or at a demo-friendly rate while watching Storm UI and database counts.
