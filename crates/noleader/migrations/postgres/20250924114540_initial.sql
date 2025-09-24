-- Add migration script here

CREATE TABLE IF NOT EXISTS noleader_leaders (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL,
    revision BIGINT NOT NULL,
    heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);
