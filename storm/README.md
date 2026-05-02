# Storm streaming-layer notes

This folder contains topology and bolt skeletons for the streaming speed layer.

Implementation notes:
- You can implement the topology in Java/Scala (native Storm) or in Python using
  `streamparse` or another compatible wrapper.
- The project expects the following bolts: `ParseBolt`, `DistrictBolt`, `WindowBolt`, `AnomalyBolt`, `AlertBolt`.
- `AlertBolt` is responsible for writing to PostgreSQL `alerts` table and MongoDB `alert_logs` collection.
- Raw event logging to `raw_events` must be disabled by default; enable TTL via MongoDB index if enabled.

File stubs are provided as starting points. Replace with real bolt implementations before integration.
