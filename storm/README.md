# Storm streaming-layer notes

This folder contains topology and bolt skeletons for the streaming speed layer.

Implementation notes:
- The current deployment path uses a Java Storm topology jar with a Kafka spout feeding the bolt chain.
- The project expects the following components: `CrimeAlertTopology`, `ParseBolt`, `DistrictBolt`, `WindowBolt`, `AnomalyBolt`, `AlertBolt`.
- `AlertBolt` is responsible for writing to PostgreSQL `alerts` table and MongoDB `alert_logs` collection.
- Raw event logging to `raw_events` must be disabled by default; enable TTL via MongoDB index if enabled.

File stubs are provided as starting points. The topology is now wired for a real Storm cluster submission path.
