-- init.sql
CREATE TABLE IF NOT EXISTS raw_events (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_raw_events_received_at ON raw_events(received_at DESC);
CREATE INDEX idx_raw_events_device_id ON raw_events(device_id);
CREATE INDEX idx_alerts_created_at ON alerts(created_at DESC);