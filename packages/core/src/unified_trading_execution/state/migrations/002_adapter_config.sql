-- 002: adapter_config — key/value store for per-adapter platform-specific intent
-- (e.g. Bybit leverage values, margin mode per symbol).
-- Applied by the migration runner in store.py on first initialize().
-- Runs after 001_initial.sql.

CREATE TABLE IF NOT EXISTS adapter_config (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,          -- JSON-encoded
    updated_at TEXT NOT NULL           -- ISO-8601 UTC
);
