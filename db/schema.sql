CREATE TABLE IF NOT EXISTS traffic_metrics(
    window_start TIMESTAMP,
    requests_per_min BIGINT,
    error_rate DOUBLE PRECISION,
    avg_response_time DOUBLE PRECISION,
    unique_users BIGINT,
    is_anomaly BOOLEAN
)