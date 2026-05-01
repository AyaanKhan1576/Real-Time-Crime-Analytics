# Testing Guide: Kafka + Storm Speed Layer (Person 2)

## Project Status for Person 2

### ✓ COMPLETED
- **Kafka Producer** (`kafka/producer.py`): Reads CSV, publishes to `crime-events` topic with normalization
- **Storm Bolts** (all 5): ParseBolt, DistrictBolt, WindowBolt, AnomalyBolt, AlertBolt
  - Full schema validation, district normalization, sliding window, anomaly detection
- **Database Setup**: Postgres schema (`db/postgres_init.sql`), Mongo indexes (`db/mongo_setup.py`)
- **Config System**: `config/config.yaml.example` (tracked), `config/config.yaml` (ignored, local copy)
- **In-Process Harness**: `storm/harness/run_topology_runner.py` (test-mode topology runner)
- **Docker Compose**: Services for Kafka, Zookeeper, Postgres, Mongo, and now storm-harness
- **Scripts**: Packaging, initialization, validation, smoke tests

### ⚠ NEAR-COMPLETE (needs integration verification)
- **Full Kafka → Storm → DB integration**: Code is ready; needs testing against real running services
- **Streamlit Dashboard** (optional): Bonus feature, not yet implemented

### ⊘ OUT OF SCOPE
- **Person 1 Batch Layer** (Spark, HDFS): Person 1's responsibility
- **DevOps/Production Deployment**: Use docker-compose locally; production setup TBD

---

## How Testing Works

### **1. Smoke Test (In-Process Harness)**
**Purpose**: Verify bolts' logic without requiring full Storm or databases.

**What gets tested**:
- ParseBolt: JSON parsing, field validation, district normalization
- DistrictBolt: Routing by district
- WindowBolt: Sliding window counting
- AnomalyBolt: Severity classification (LOW/MEDIUM/HIGH based on event counts)

**Run**:
```bash
python scripts/run_quick_smoke.py        # ~1% of dataset (~85k rows)
python scripts/integration_smoke_test.py # ~25% of dataset (~2M rows)
```

**Success Criteria**:
```
Running quick smoke (1% of dataset)...
CSV rows: 8542305, processing ~85423
Processed 85423 rows.
Quick smoke finished
```
✓ No crashes or unhandled exceptions
✓ All rows processed  
✓ (Optional) Check logs for anomalies printed

---

### **2. Full Integration: Docker Compose**
**Purpose**: Test Kafka → Harness → Outputs in a containerized local environment.

**Services**:
- Zookeeper: Kafka coordinator
- Kafka: Message broker (runs `crime-events` topic)
- Postgres: Stores `realtime_district_counts` and `alerts`
- Mongo: Stores `alert_logs`
- storm-harness: In-process topology runner consuming Kafka

**Setup**:
```bash
# 1. Create local config
cp config/config.yaml.example config/config.yaml
# Edit config.yaml if needed (defaults should work with docker-compose):
# - kafka.bootstrap_servers: "kafka:9092" (use docker service name, not localhost)
# - postgres.host: "postgres"
# - mongodb.host: "mongo"

# 2. Start all services
docker-compose -f docker/docker-compose.yml up -d

# 3. Verify services are healthy
docker-compose -f docker/docker-compose.yml ps
# All should show "Up"

# 4. Check logs (optional)
docker-compose -f docker/docker-compose.yml logs kafka
docker-compose -f docker/docker-compose.yml logs storm-harness
```

**Success Criteria**:
```bash
# Check storm-harness logs
docker-compose -f docker/docker-compose.yml logs storm-harness

# Expected output:
# Full integration test: Kafka → harness → file outputs
# ✓ Connected to Kafka
# CSV rows: 8542305, processing ~2101447
# ✓ Harness processing completed
# ✓ Integration test complete. Outputs in logs/
#   - anomalies.jsonl: N records
#   - summary.json: 1 records
```

✓ Docker services all start without errors  
✓ storm-harness connects to Kafka  
✓ Output files written to `logs/` volume (visible as `docker/logs/`)  
✓ Anomalies detected and logged  

### **2b. Live Demo Monitor**
**Purpose**: Show a moving picture of the stack during your presentation.

Run this in a separate terminal while Docker is up:
```bash
python scripts/demo_monitor.py
```

What it shows:
- current `docker-compose ps` state
- latest `storm-harness` log lines
- live Postgres row counts for `realtime_district_counts` and `alerts`
- Mongo `alert_logs` document count

If you want the database rows to increase during the final Docker run, keep `PERSIST_TO_DBS=1` for `scripts/run_full_integration.py`.

---

### **3. Full Kafka → Storm → Postgres/Mongo (Real Storm Cluster)**
**Purpose**: Test persistence layer writes.

**Setup**:
```bash
# Start services
docker-compose -f docker/docker-compose.yml up -d

# Wait for Postgres to be ready
sleep 10

# Initialize DB schemas
docker exec $(docker ps -q -f ancestor=postgres:14) psql -U crime_user -d crime_analytics -f /docker-entrypoint-initdb.d/postgres_init.sql

docker exec $(docker ps -q -f ancestor=mongo:6) bash -c "python3 - <<'PY'
from pymongo import MongoClient
client = MongoClient('localhost', 27017)
db = client.get_database('crime_analytics')
db.alert_logs.create_index('alert_id', unique=True)
print('Mongo indexes ready')
PY"

# Start producer (send events to Kafka)
python kafka/producer.py --config config/config.yaml --max-rows 2000

# Deploy real Storm topology (requires streamparse or Storm CLI)
# (For now, use the in-process harness or deploy via streamparse)

# Query results
psql "postgresql://crime_user:crime_password@localhost:5432/crime_analytics" -c \
  "SELECT district, event_count, window_start, window_end FROM realtime_district_counts LIMIT 20;"

psql "postgresql://crime_user:crime_password@localhost:5432/crime_analytics" -c \
  "SELECT alert_id, district, severity, message FROM alerts LIMIT 20;"

# Mongo
mongosh "mongodb://localhost:27017/crime_analytics" --eval \
  "db.alert_logs.find().sort({timestamp:-1}).limit(10).forEach(printjson)"
```

**Success Criteria**:
✓ Postgres tables populated with rows  
✓ Anomalies detected (severity = LOW/MEDIUM/HIGH)  
✓ Mongo documents written for detected anomalies  

---

## Testing Checklist

### Pre-Test
- [ ] Clone repo and checkout `minahil` branch
- [ ] Copy `config/config.yaml.example` → `config/config.yaml`
- [ ] Verify Python packages: `pip install pyyaml kafka-python psycopg2-binary pymongo`
- [ ] Verify Docker is running and docker-compose is available

### Smoke Test (Quick ~5 min)
```bash
python scripts/run_quick_smoke.py
```
- [ ] Completes without exceptions
- [ ] Prints "Processed ~85k rows"

### Docker Integration (15-30 min)
```bash
docker-compose -f docker/docker-compose.yml up -d
# Wait 30 seconds for services to start
docker-compose -f docker/docker-compose.yml logs storm-harness
# Within 1-2 minutes:
```
- [ ] storm-harness logs show "Connected to Kafka"
- [ ] "Harness processing completed"
- [ ] "Outputs in logs/" with anomalies.jsonl and summary.json

### Producer → Kafka Test (30 min)
```bash
python kafka/producer.py --config config/config.yaml --max-rows 200 --kafka-broker kafka:9092 &
# (In another terminal) Monitor Kafka topic:
docker-compose -f docker/docker-compose.yml exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic crime-events --from-beginning --max-messages 10
```
- [ ] Messages appear in Kafka topic
- [ ] Each message is valid JSON with required fields

### Postgres Verification
```bash
docker-compose -f docker/docker-compose.yml exec -T postgres psql -U crime_user -d crime_analytics -c "\\dt"
docker-compose -f docker/docker-compose.yml exec -T postgres psql -U crime_user -d crime_analytics -c "SELECT COUNT(*) FROM realtime_district_counts;"
docker-compose -f docker/docker-compose.yml exec -T postgres psql -U crime_user -d crime_analytics -c "SELECT * FROM alerts LIMIT 5;"
```
- [ ] realtime_district_counts table has rows
- [ ] alerts table has rows with severity values

Note: `\\dt` is a psql meta-command, so it must be run by itself or with separate `-c` flags. Do not put SQL statements on the same line after `\\dt`.

### Mongo Verification
```bash
docker-compose -f docker/docker-compose.yml exec -T mongo mongosh --eval "db.getSiblingDB('crime_analytics').alert_logs.countDocuments()"
docker-compose -f docker/docker-compose.yml exec -T mongo mongosh --eval "db.getSiblingDB('crime_analytics').alert_logs.findOne()"
```
- [ ] alert_logs collection has documents
- [ ] Documents have expected schema (alert_id, severity, timestamp, etc.)

---

## Expected Results

### Smoke Test
```json
{
  "timestamp": 1714580000.0,
  "total_rows_processed": 85423,
  "anomalies_detected": 42,
  "sample_fraction": 0.01
}
```

### Full Integration (25% sample ≈ 2.1M rows)
- **Anomalies detected**: 150-300 (varies by data and thresholds)
- **Districts represented**: 20-30 unique districts
- **Severity breakdown**:
  - LOW: 40-50%
  - MEDIUM: 30-40%
  - HIGH: 10-20%

### Threshold Config (from `config/config.yaml`)
```yaml
storm:
  window_size_seconds: 300          # 5-min sliding window
  slide_interval_seconds: 60        # emit every 60 seconds
  anomaly_threshold: 25             # baseline threshold
  severity:
    low_min: 25                     # event_count >= 25 → LOW
    medium_min: 40                  # event_count >= 40 → MEDIUM
    high_min: 60                    # event_count >= 60 → HIGH
```

---

## Troubleshooting

### Error: "No module named 'yaml'"
```bash
pip install pyyaml
```

### Error: "Connection refused" (Kafka)
- Check: `docker-compose ps` (is Kafka service running?)
- Wait: Kafka takes 20-30 seconds to start
- Config: Ensure `bootstrap_servers: kafka:9092` in docker context (not localhost)

### Error: "Postgres connection failed"
```bash
docker-compose -f docker/docker-compose.yml logs postgres
# If it didn't run the init script:
docker exec $(docker ps -q -f ancestor=postgres:14) psql -U crime_user -d crime_analytics -f /docker-entrypoint-initdb.d/postgres_init.sql
```

### Empty Alerts Table
- Check thresholds in config.yaml (if window event counts never exceed 25, no LOW alerts)
- Run producer with more rows: `--max-rows 5000`
- Verify window_size_seconds (300 = 5 min; adjust if dataset is small)

---

## Next Steps

1. **Run smoke test** to verify bolts logic ✓ (quick)
2. **Run docker-compose** to verify full pipeline ✓ (15-30 min)
3. **Deploy real Storm topology** when ready:
   - Use streamparse: `streamparse deploy` (requires streamparse installed)
   - Or use Storm CLI with packaged JAR (Person 1 may handle this)
4. **Monitor dashboard** (optional, Streamlit integration pending)

---

## References

- **Config**: [config/config.yaml.example](../config/config.yaml.example)
- **Producer**: [kafka/producer.py](../kafka/producer.py)
- **Bolts**: [storm/bolts/](../storm/bolts/)
- **Harness**: [storm/harness/run_topology_runner.py](../storm/harness/run_topology_runner.py)
- **Docker**: [docker/docker-compose.yml](../docker/docker-compose.yml)
