# Storm Topology Contract

The speed layer must implement:

```text
KafkaSpout -> ParseBolt -> DistrictBolt -> WindowBolt -> AnomalyBolt -> AlertBolt
```

It owns only:

```text
alerts
realtime_district_counts
MongoDB alert_logs
MongoDB raw_events
```

It must consume Kafka topic `crime-events` and use the normalized JSON fields:

```text
case_number, date, block, primary_type, district, arrest, latitude, longitude
```

`district` must always remain a string. Alert IDs must follow:

```text
{district}_{window_start}_{window_end}
```
