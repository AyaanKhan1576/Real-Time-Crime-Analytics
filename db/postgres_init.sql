-- Speed layer tables (Person 2 ownership)

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    alert_id VARCHAR(200) UNIQUE NOT NULL,
    district VARCHAR(50) NOT NULL,
    alert_timestamp TIMESTAMP NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_count BIGINT NOT NULL,
    threshold_value BIGINT NOT NULL,
    severity VARCHAR(30) NOT NULL,
    message TEXT,
    source VARCHAR(50) DEFAULT 'storm',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_district
ON alerts(district);

CREATE INDEX IF NOT EXISTS idx_alerts_timestamp
ON alerts(alert_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_window
ON alerts(window_start, window_end);

CREATE INDEX IF NOT EXISTS idx_alerts_severity
ON alerts(severity);

CREATE TABLE IF NOT EXISTS realtime_district_counts (
    id SERIAL PRIMARY KEY,
    district VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_count BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_realtime_counts_window
ON realtime_district_counts(district, window_start, window_end);

CREATE INDEX IF NOT EXISTS idx_realtime_counts_district
ON realtime_district_counts(district);

CREATE INDEX IF NOT EXISTS idx_realtime_counts_window
ON realtime_district_counts(window_end DESC);
