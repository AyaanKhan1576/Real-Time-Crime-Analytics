CREATE TABLE IF NOT EXISTS batch_job_status (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    job_name VARCHAR(100) NOT NULL,
    status VARCHAR(30) NOT NULL,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    last_successful_run TIMESTAMP,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_batch_status CHECK (status IN ('running', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_batch_run
ON batch_job_status(run_id);

CREATE INDEX IF NOT EXISTS idx_batch_status
ON batch_job_status(status);

CREATE INDEX IF NOT EXISTS idx_batch_finished_at
ON batch_job_status(finished_at DESC);

CREATE TABLE IF NOT EXISTS crime_trends (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    trend_type VARCHAR(50) NOT NULL,
    trend_key VARCHAR(100) NOT NULL,
    crime_count BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crime_trends_run
ON crime_trends(run_id);

CREATE INDEX IF NOT EXISTS idx_crime_trends_type
ON crime_trends(trend_type);

CREATE INDEX IF NOT EXISTS idx_crime_trends_run_type
ON crime_trends(run_id, trend_type);

CREATE TABLE IF NOT EXISTS crime_trends_temp (
    run_id VARCHAR(100) NOT NULL,
    trend_type VARCHAR(50) NOT NULL,
    trend_key VARCHAR(100) NOT NULL,
    crime_count BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS arrest_rates (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    primary_type VARCHAR(100),
    district VARCHAR(50),
    race VARCHAR(100),
    total_crimes BIGINT,
    total_arrests BIGINT,
    arrest_rate DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_arrest_rates_run
ON arrest_rates(run_id);

CREATE INDEX IF NOT EXISTS idx_arrest_rates_district
ON arrest_rates(district);

CREATE INDEX IF NOT EXISTS idx_arrest_rates_primary_type
ON arrest_rates(primary_type);

CREATE TABLE IF NOT EXISTS arrest_rates_temp (
    run_id VARCHAR(100) NOT NULL,
    primary_type VARCHAR(100),
    district VARCHAR(50),
    race VARCHAR(100),
    total_crimes BIGINT,
    total_arrests BIGINT,
    arrest_rate DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS violence_stats (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    month VARCHAR(30),
    district VARCHAR(50),
    community_area VARCHAR(100),
    total_homicides BIGINT,
    total_nonfatal_shootings BIGINT,
    gunshot_incidents BIGINT,
    total_incidents BIGINT,
    gunshot_proportion DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_violence_stats_run
ON violence_stats(run_id);

CREATE INDEX IF NOT EXISTS idx_violence_stats_district
ON violence_stats(district);

CREATE INDEX IF NOT EXISTS idx_violence_stats_month
ON violence_stats(month);

CREATE TABLE IF NOT EXISTS violence_stats_temp (
    run_id VARCHAR(100) NOT NULL,
    month VARCHAR(30),
    district VARCHAR(50),
    community_area VARCHAR(100),
    total_homicides BIGINT,
    total_nonfatal_shootings BIGINT,
    gunshot_incidents BIGINT,
    total_incidents BIGINT,
    gunshot_proportion DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sex_offender_density (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    district VARCHAR(50),
    district_name VARCHAR(255),
    offender_count BIGINT,
    victim_minor_count BIGINT,
    density_rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sex_density_run
ON sex_offender_density(run_id);

CREATE INDEX IF NOT EXISTS idx_sex_density_district
ON sex_offender_density(district);

CREATE TABLE IF NOT EXISTS sex_offender_density_temp (
    run_id VARCHAR(100) NOT NULL,
    district VARCHAR(50),
    district_name VARCHAR(255),
    offender_count BIGINT,
    victim_minor_count BIGINT,
    density_rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hotspots (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    cluster_id INTEGER NOT NULL,
    centroid_latitude DOUBLE PRECISION,
    centroid_longitude DOUBLE PRECISION,
    crime_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hotspots_run
ON hotspots(run_id);

CREATE INDEX IF NOT EXISTS idx_hotspots_cluster
ON hotspots(cluster_id);

CREATE TABLE IF NOT EXISTS hotspots_temp (
    run_id VARCHAR(100) NOT NULL,
    cluster_id INTEGER NOT NULL,
    centroid_latitude DOUBLE PRECISION,
    centroid_longitude DOUBLE PRECISION,
    crime_count BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS correlations (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    correlation_name VARCHAR(255) NOT NULL,
    grouping_key VARCHAR(100),
    x_metric VARCHAR(100),
    y_metric VARCHAR(100),
    x_value DOUBLE PRECISION,
    y_value DOUBLE PRECISION,
    correlation_value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_correlations_run
ON correlations(run_id);

CREATE INDEX IF NOT EXISTS idx_correlations_name
ON correlations(correlation_name);

CREATE TABLE IF NOT EXISTS correlations_temp (
    run_id VARCHAR(100) NOT NULL,
    correlation_name VARCHAR(255) NOT NULL,
    grouping_key VARCHAR(100),
    x_metric VARCHAR(100),
    y_metric VARCHAR(100),
    x_value DOUBLE PRECISION,
    y_value DOUBLE PRECISION,
    correlation_value DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
