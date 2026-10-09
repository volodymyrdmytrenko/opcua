CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    protocol TEXT NOT NULL,
    endpoint TEXT NULL,
    host TEXT NULL,
    port INTEGER NULL,
    unit_id INTEGER NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tags (
    tag_name TEXT PRIMARY KEY,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    data_type TEXT NOT NULL,
    source_address TEXT NOT NULL,
    writable BOOLEAN NOT NULL DEFAULT FALSE,
    history_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    history_mode TEXT NOT NULL DEFAULT 'disabled',
    history_interval_ms INTEGER NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tag_history (
    time TIMESTAMPTZ NOT NULL,
    tag_name TEXT NOT NULL,
    device_id TEXT NOT NULL,
    data_type TEXT NOT NULL,
    value_bool BOOLEAN NULL,
    value_int BIGINT NULL,
    value_double DOUBLE PRECISION NULL,
    value_text TEXT NULL,
    quality TEXT NOT NULL DEFAULT 'Good'
);

CREATE INDEX IF NOT EXISTS ix_tag_history_tag_time
ON tag_history(tag_name, time DESC);

CREATE TABLE IF NOT EXISTS write_log (
    id BIGSERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tag_name TEXT NOT NULL,
    device_id TEXT NOT NULL,
    requested_value TEXT NULL,
    success BOOLEAN NOT NULL,
    error_message TEXT NULL
);

CREATE INDEX IF NOT EXISTS ix_write_log_tag_time
ON write_log(tag_name, time DESC);
