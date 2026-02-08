CREATE TABLE IF NOT EXISTS traffic_metrics(
    window_start TIMESTAMP,
    requests_per_min INTEGER,
    error_rate DOUBLE PRECISION,
    avg_response_time DOUBLE PRECISION,
    unique_users INTEGER,
    is_anomaly BOOLEAN
)