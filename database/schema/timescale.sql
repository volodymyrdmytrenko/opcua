CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS tag_history (
 time TIMESTAMPTZ NOT NULL,
 device_id TEXT NOT NULL,
 tag_name TEXT NOT NULL,
 data_type TEXT NOT NULL,
 value_bool BOOLEAN NULL,
 value_int BIGINT NULL,
 value_double DOUBLE PRECISION NULL,
 value_text TEXT NULL,
 quality TEXT NOT NULL DEFAULT 'Good'
);

SELECT create_hypertable('tag_history', by_range('time'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS ix_tag_history_device_tag_time
ON tag_history (device_id, tag_name, time DESC);
