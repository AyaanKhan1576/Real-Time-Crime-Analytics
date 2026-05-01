**Run Summary**

- **Action**: Completed an unthrottled replay of the full crime CSV into the cluster (Kafka → Storm) and verified downstream writes.

**Results**
- **Postgres - alerts**: 22
- **Postgres - realtime_district_counts**: 44
- **MongoDB - alert_logs**: 22
- **MongoDB - raw_events**: 0

**How I ran it**
- **Topology submit**: Java topology submitted via the `storm-submit-java` compose profile (uses the shaded jar built from [java-topology](java-topology)).
- **Producer**: Ran the cluster-local producer inside the `storm-harness` service so it resolves the `kafka` hostname and uses container networking. I set env overrides so the producer read the actual dataset file: `DATA__BASE_PATH=/app/data/raw` and `DATA__CRIME_FILE=Crimes_-_2001_to_Present_20260501.csv`.
- **Key files**: configuration used: [config/config.yaml](config/config.yaml)

**Commands run (core)**
- Start cluster (compose): `docker compose -f docker/docker-compose.yml up -d`.
- Submit topology: use the `storm-submit-java` profile in compose.
- Run producer (in-compose): `docker compose -f docker/docker-compose.yml run --rm -e DATA__BASE_PATH=/app/data/raw -e DATA__CRIME_FILE=Crimes_-_2001_to_Present_20260501.csv -e KAFKA__PRODUCER_RATE_PER_SECOND=0 storm-harness bash -lc "python kafka/producer.py --config config/config.yaml"`
- Postgres checks: `docker exec docker-postgres-1 psql -U crime_user -d crime_analytics -c "select 'alerts' as table_name, count(*) from alerts union all select 'realtime_district_counts', count(*) from realtime_district_counts;"`
- Mongo checks: `docker exec docker-mongo-1 mongosh crime_analytics --quiet --eval "printjson({alert_logs: db.alert_logs.countDocuments(), raw_events: (db.raw_events ? db.raw_events.countDocuments() : 0)})"`

**Notes & observations**
- The producer required the dataset filename to match what's mounted inside the container; I passed env overrides to avoid changing repository config.
- The topology wrote matching counts to Postgres and Mongo: `alert_logs` equals `alerts` (22) and there are district counts (44 rows) in `realtime_district_counts`.
- `raw_events` collection remains empty in Mongo (0) because the current Storm topology only writes `alert_logs` for detected alerts.
- After a later config change, Storm UI stopped loading because the containers tried to `chown` a read-only mounted `storm.yaml`. I fixed that by baking `storm.yaml` into a Storm cluster image and setting `STORM_CONF_DIR=/conf`, then rebuilding `storm-nimbus`, `storm-supervisor`, and `storm-ui`.
- The producer import error (`ModuleNotFoundError: No module named 'config'`) was fixed by inserting the repo root into `sys.path` inside `kafka/producer.py` and setting `PYTHONPATH=/app` in the harness image.
- The replay initially still sent `0` records because the CSV headers were mixed-case (`Case Number`, `Primary Type`, etc.) while the producer only matched exact all-caps keys. I fixed this by normalizing header names before extraction, then rebuilding the harness image.
- After the fix, a short replay test with `--max-rows 5` successfully sent 5 records to Kafka.
- A separate Storm runtime issue showed up in the UI API: `district-bolt` was failing with `java.util.LinkedHashMap` not registered in Kryo. I fixed this by registering `LinkedHashMap`, `HashMap`, and `ArrayList` in `CrimeAlertTopology`, then killing and resubmitting the topology so the new jar was actually deployed.

Next steps available:
- Commit this `report.md` and push.
- Run a throttled replay or a replay for a subset for faster verification.
- Produce a longer formal report with architecture diagrams and a step-by-step troubleshooting log.

**Full Report — Detailed**

**Summary of what was done**
- Brought up the local cluster via Docker Compose (Zookeeper, Kafka, Storm Nimbus/Supervisor/UI, Postgres, MongoDB).
- Built and submitted the Java Storm topology (shaded JAR) to the Storm cluster (`crime-alert-topology`).
- Ran the Kafka producer inside the compose network to publish the full crime CSV into Kafka at full speed (unthrottled, `rate=0`).
- Verified downstream writes: Postgres `alerts` and `realtime_district_counts`; Mongo `alert_logs`.

**Results (quick)**
- Postgres `alerts`: 22
- Postgres `realtime_district_counts`: 44
- Mongo `alert_logs`: 22
- Mongo `raw_events`: 0

**High-level architecture**
This project implements a Lambda-style pattern with a streaming speed layer implemented in Apache Storm. The major components and their responsibilities:

- Kafka: durable event ingestion and buffering. Topic used: `crime-events`.
- Storm: speed layer for real-time detection/alerting. Topology reads from Kafka, processes events (parse → anomaly/district/window logic → alert), and writes to Postgres and MongoDB.
- Java topology: native Storm topology written in Java (Maven), packaged as a shaded JAR for cluster submission. Replaces streamparse-based Python submit path to avoid platform build issues.
- Kafka producer (Python): reads CSV rows, normalizes fields, and publishes JSON events to the Kafka topic. Designed to run inside the compose network so `kafka:9092` resolves.
- Postgres: relational sink for structured alert records (`alerts`) and aggregated district counts (`realtime_district_counts`). Schema in `db/postgres_init.sql`.
- MongoDB: document sink for `alert_logs` (raw alert documents for downstream usage) and optionally `raw_events`.
- Docker Compose: orchestrates the above services and provides different profiles for submit/harness modes.
- Storm harness (Python image): convenience service for running producer and dev harness; not used for production submission but useful for in-network runs.

**Detailed component notes**

- Kafka
	- Role: the single source of truth for the event stream; supports replay of historical CSV by publishing events in order.
	- Key config: `bootstrap.servers` configured as `kafka:9092` inside the compose network.
	- Notes: Producer must run within the compose network (or container with network access to `kafka`) or use correct host mapping.

- Storm (Nimbus / Supervisor / UI)
	- Role: consumes from Kafka, executes the topology (spouts & bolts), and writes alerts/aggregates.
	- Java topology: packaged with `storm-kafka-client` shaded into the JAR to avoid NoClassDefFoundError during submission.
	- Important runtime config: `storm.yaml` in the cluster image must set `nimbus.seeds` to the compose service name `storm-nimbus` so the UI, Nimbus, and submit client all resolve the same leader.
	- Deployment detail: `storm-nimbus`, `storm-supervisor`, and `storm-ui` now use a dedicated image that copies `java-topology/storm.yaml` into `/conf` instead of a read-only bind mount.

- Java Topology & Bolts
	- `CrimeAlertTopology` wires Kafka spout → parse bolt → anomaly/detection → alert bolt → sink(s).
	- `AlertBolt` was refactored to avoid non-serializable fields and to initialize DB connections in `prepare()`.
	- Packaging: `maven-shade-plugin` ensures Kafka spout classes are included in the final JAR.

- Producer (Python)
	- Reads `config/config.yaml`, but env variable overlays are supported (e.g., `DATA__BASE_PATH`, `DATA__CRIME_FILE`, `KAFKA__PRODUCER_RATE_PER_SECOND`).
	- Can run fast/unthrottled by setting rate to `0`.
	- Must resolve `config` module; the current fix inserts the repo root into `sys.path` and also sets `PYTHONPATH=/app` in the harness image so `python kafka/producer.py ...` works consistently inside compose.
	- CSV headers are normalized to lowercase/underscored names so mixed-case fields like `Case Number` and `Primary Type` are parsed correctly.

- Postgres
	- Stores normalized `alerts` rows and `realtime_district_counts` aggregates used for serving.
	- Initialization script located at `db/postgres_init.sql`.

- MongoDB
	- Stores `alert_logs` (alert documents) for flexible querying and downstream analysis.

- Docker Compose
	- Compose file: `docker/docker-compose.yml` contains multiple profiles including `java-submit` and `storm-harness`.
	- Use the `java-submit` profile to build/submit the Java topology image; use `storm-harness` profile to run the producer inside the same network.

**Challenges faced and how they were fixed**

- streamparse / thriftpy native build failure on host/WSL
	- Symptom: `streamparse` failed to compile due to native dependencies and toolchain mismatch.
	- Fix: Switched to a Java-native Storm topology and submit path; removed the dependency on `streamparse` for production submission.

- Java compile/runtime errors for Kafka spout classes
	- Symptom: `NoClassDefFoundError: org/apache/storm/kafka/spout/KafkaSpoutConfig` during submission.
	- Fix: Include `storm-kafka-client` and other required classes in the shaded JAR (adjusted `pom.xml` + `maven-shade-plugin`).

- Non-serializable bolt fields causing Storm submit failures
	- Symptom: StormSubmitter complained about non-serializable fields (e.g., passing a `TopologyConfig` object into `AlertBolt`).
	- Fix: Refactor bolts to accept primitive/string configuration and initialize DB clients inside `prepare()`.

- Nimbus discovery failure from submit container
	- Symptom: submit client couldn't locate the Nimbus leader.
	- Fix: Add `storm.yaml` into the submit image with `nimbus.seeds: ["storm-nimbus"]` and ensure compose service name matches.

- Storm UI crash after read-only config mount
	- Symptom: `storm-ui` and `storm-nimbus` exited with `chown: changing ownership of '/conf/storm.yaml': Read-only file system` and the UI returned a `500 Server Error`.
	- Fix: Replace the bind-mounted config with a custom Storm image that copies `storm.yaml` into `/conf` at build time and sets `STORM_CONF_DIR=/conf`.
	- Validation: Storm UI responded again on `http://localhost:8080` after recreating the cluster services.

- Harness image failed to build because of streamparse
	- Symptom: the harness image tried to install `streamparse==3.6.0`, which pulled in `thriftpy` and failed during wheel build.
	- Fix: Remove `streamparse` from the harness image because the producer path does not need it; keep only the runtime libraries required for the producer and local harness.
	- Validation: `docker compose -f docker/docker-compose.yml run --rm storm-harness python kafka/producer.py --help` now succeeds.

- Producer dataset filename mismatch causing early abort
	- Symptom: Producer expected `data/raw/crimes.csv` but dataset file was `Crimes_-_2001_to_Present_20260501.csv`.
	- Fix: Used environment variable overlays to point `DATA__BASE_PATH` and `DATA__CRIME_FILE` to the actual filename when running inside the compose network.

- Producer emitted 0 rows because the CSV column names were mixed-case
	- Symptom: the replay completed with `Sent 0 records` even though the file contained rows.
	- Fix: Normalize CSV headers in `kafka/producer.py` before reading `case_number`, `date`, `block`, `primary_type`, `district`, `arrest`, `latitude`, and `longitude`.
	- Validation: a short replay with `--max-rows 5` sent 5 records successfully.

- Storm topology was active but not processing due to Kryo serialization
	- Symptom: Storm UI showed `java.lang.IllegalArgumentException: Class is not registered: java.util.LinkedHashMap` on `district-bolt` and the topology summary stayed at zero emitted/acked.
	- Fix: Register `java.util.LinkedHashMap`, `java.util.HashMap`, and `java.util.ArrayList` in `CrimeAlertTopology`, then kill and resubmit the topology so the fixed jar was loaded.
	- Validation: Nimbus `storm list` showed a fresh topology id after resubmission.

- Networking / NoBrokersAvailable when running producer from host
	- Symptom: `kafka.errors.NoBrokersAvailable` when running a docker container not attached to compose network.
	- Fix: Run the producer inside the compose network (e.g., `docker compose run --rm storm-harness`) so `kafka:9092` resolves.

**Commands — full deploy & run (copyable)**

Start the cluster (background):
```bash
docker compose -f docker/docker-compose.yml up -d
```

```bash
docker compose -f docker/docker-compose.yml down -v
```

Build submit image (java submit profile):
```bash
docker compose -f docker/docker-compose.yml --profile java-submit build storm-submit-java
```

Submit the Java topology (example pattern already used):
```bash
docker compose -f docker/docker-compose.yml --profile java-submit up -d storm-submit-java
# or run the submit image which will call StormSubmitter with the shaded JAR
```

Run the producer inside the compose network (unthrottled/full replay):
```bash
docker compose -f docker/docker-compose.yml run --rm `
-e DATA__BASE_PATH=/app/data/raw `
-e DATA__CRIME_FILE=Crimes_-_2001_to_Present_20260501.csv `
-e KAFKA__PRODUCER_RATE_PER_SECOND=0 `
storm-harness bash -lc "python kafka/producer.py --config config/config.yaml"
```

```bash
docker compose -f docker/docker-compose.yml run --rm -e DATA__BASE_PATH=/app/data/raw -e DATA__CRIME_FILE=Crimes_-_2001_to_Present_20260501.csv -e KAFKA__PRODUCER_RATE_PER_SECOND=0 storm-harness python kafka/producer.py --config config/config.yaml --max-rows 5
```

Verify Postgres counts:
```bash
docker exec docker-postgres-1 psql -U crime_user -d crime_analytics -c "select 'alerts' as table_name, count(*) from alerts union all select 'realtime_district_counts', count(*) from realtime_district_counts;"
```

Verify Mongo counts:
```bash
docker exec docker-mongo-1 mongosh crime_analytics --quiet --eval "printjson({alert_logs: db.alert_logs.countDocuments(), raw_events: (db.raw_events ? db.raw_events.countDocuments() : 0)})"
```

Collect Storm submit logs (example):
```bash
docker compose -f docker/docker-compose.yml logs --no-color --tail 200 storm-submit-java
```

Restart Storm after config changes:
```bash
docker compose -f docker/docker-compose.yml restart storm-nimbus storm-supervisor storm-ui
```

If the UI still needs a full recreate:
```bash
docker compose -f docker/docker-compose.yml up -d --force-recreate storm-nimbus storm-supervisor storm-ui
```

Resubmit the topology after code changes:
```bash
docker exec docker-storm-nimbus-1 storm kill crime-alert-topology -w 1
docker compose -f docker/docker-compose.yml --profile java-submit up -d --force-recreate storm-submit-java
```

**Harness vs Cluster — usage & purpose**

- Harness (dev):
	- Purpose: convenience development and testing. The `storm-harness` profile provides a lightweight container with the repo mounted and Python tooling preinstalled allowing fast iteration of the producer and local harness tests.
	- When to use: debugging, small-scale tests, and running the producer inside the compose network for connectivity to Kafka.

- Cluster (production-like):
	- Purpose: realistic end-to-end run using Storm's JVM-based runtime (Nimbus/Supervisor) and the Java topology submit path; suitable for performance testing, realistic scheduling, and multi-worker parallelism.
	- When to use: final validation, performance/load runs, and demonstration of the deployment path that matches a real Storm cluster.

**What was used and why (project fit)**

- Kafka: required for event ingestion and reliable replay of historical data.
- Storm (Java topology): chosen because `streamparse`/Python submission failed to build reliably in the environment; Java submit is the supported, production-ready Storm submit path.
- Java + Maven + shade: ensures a single JAR with all runtime deps so Storm can distribute it to workers.
- Postgres + MongoDB: used as complementary sinks — Postgres for structured, relational serving and MongoDB for flexible alert logs.
- Docker Compose: reproduces the cluster locally and enables isolated networked services for end-to-end testing.

**Appendix — troubleshooting notes & tips**

- If you see `NoClassDefFoundError` for Kafka spout classes during submit, re-check the `pom.xml` and the shade plugin to ensure `storm-kafka-client` is packaged.
- If submit fails to find Nimbus, confirm the `storm.yaml` inside the submit image sets `nimbus.seeds` to `storm-nimbus` and the compose service name matches that seed.
- To replay a subset quickly for validation, set `KAFKA__PRODUCER_RATE_PER_SECOND=1000` (or a small number) or write a small head-only CSV and point `DATA__CRIME_FILE` to it.

**Final note**
I updated this repository's `report.md` with the above full details and appended the quick results. Tell me if you want this committed and pushed, or if you want an expanded PDF-style report including architecture diagrams and a full step-by-step timeline of fixes.
