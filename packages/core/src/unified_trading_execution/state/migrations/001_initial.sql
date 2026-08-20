-- 001: Initial schema — positions, balances, orders, fills, audit, reconciliation, halts.
-- Applied by the migration runner in store.py on first initialize().

PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

-- ============================================================
-- Current state
-- ============================================================

CREATE TABLE IF NOT EXISTS positions (
    symbol              TEXT NOT NULL,
    quote_currency      TEXT,
    asset_class         TEXT NOT NULL,
    exchange            TEXT,
    currency            TEXT,
    expiry              TEXT,
    strike              TEXT,
    option_right        TEXT,
    multiplier          INTEGER,
    platform_symbol TEXT,
    quantity            TEXT NOT NULL,
    average_entry_price TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (symbol, asset_class)
);

CREATE TABLE IF NOT EXISTS balances (
    currency    TEXT PRIMARY KEY,
    free        TEXT NOT NULL,
    used        TEXT NOT NULL,
    total       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    client_order_id     TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    quote_currency      TEXT,
    asset_class         TEXT NOT NULL,
    exchange            TEXT,
    currency            TEXT,
    expiry              TEXT,
    strike              TEXT,
    option_right        TEXT,
    multiplier          INTEGER,
    platform_symbol TEXT,
    order_type          TEXT NOT NULL,
    side                TEXT NOT NULL,
    quantity            TEXT NOT NULL,
    time_in_force       TEXT NOT NULL,
    price               TEXT,
    stop_price          TEXT,
    reduce_only         INTEGER NOT NULL,
    client_tag          TEXT,
    take_profit_trigger TEXT,
    take_profit_limit   TEXT,
    stop_loss_trigger   TEXT,
    stop_loss_limit     TEXT,
    platform_order_id   TEXT,
    status              TEXT NOT NULL,
    filled_quantity     TEXT NOT NULL,
    average_fill_price  TEXT,
    correlation_id      TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

-- ============================================================
-- History (snapshot on every upsert)
-- ============================================================

CREATE TABLE IF NOT EXISTS position_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    quote_currency      TEXT,
    asset_class         TEXT NOT NULL,
    exchange            TEXT,
    currency            TEXT,
    expiry              TEXT,
    strike              TEXT,
    option_right        TEXT,
    multiplier          INTEGER,
    platform_symbol TEXT,
    quantity            TEXT NOT NULL,
    average_entry_price TEXT NOT NULL,
    recorded_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS balance_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    currency    TEXT NOT NULL,
    free        TEXT NOT NULL,
    used        TEXT NOT NULL,
    total       TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);

-- ============================================================
-- Append-only
-- ============================================================

CREATE TABLE IF NOT EXISTS fills (
    client_order_id     TEXT NOT NULL,
    platform_fill_id    TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    quote_currency      TEXT,
    asset_class         TEXT NOT NULL,
    exchange            TEXT,
    currency            TEXT,
    expiry              TEXT,
    strike              TEXT,
    option_right        TEXT,
    multiplier          INTEGER,
    platform_symbol TEXT,
    fill_quantity       TEXT NOT NULL,
    fill_price          TEXT NOT NULL,
    fill_timestamp      TEXT NOT NULL,
    fee_currency        TEXT,
    fee_amount          TEXT,
    correlation_id      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_events (
    event_id        TEXT PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    adapter_name    TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    correlation_id  TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload_json    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reconciliation_events (
    event_id        TEXT PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    adapter_name    TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    correlation_id  TEXT,
    mismatches_json TEXT NOT NULL,
    duration_ms     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS halt_events (
    event_id        TEXT PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    adapter_name    TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    correlation_id  TEXT,
    action          TEXT NOT NULL,
    scope           TEXT NOT NULL,
    symbol          TEXT,
    asset_class     TEXT,
    reason          TEXT NOT NULL,
    detail          TEXT NOT NULL,
    cleared_by      TEXT
);

-- ============================================================
-- Schema versioning
-- ============================================================

CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER PRIMARY KEY,
    applied_at  TEXT NOT NULL
);

-- ============================================================
-- Indexes for query performance
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_fills_fill_timestamp ON fills(fill_timestamp);
CREATE INDEX IF NOT EXISTS idx_fills_symbol ON fills(symbol);
CREATE INDEX IF NOT EXISTS idx_fills_client_order_id ON fills(client_order_id);
CREATE INDEX IF NOT EXISTS idx_position_history_recorded_at ON position_history(recorded_at);
CREATE INDEX IF NOT EXISTS idx_position_history_symbol ON position_history(symbol);
CREATE INDEX IF NOT EXISTS idx_balance_history_recorded_at ON balance_history(recorded_at);
CREATE INDEX IF NOT EXISTS idx_balance_history_currency ON balance_history(currency);
CREATE INDEX IF NOT EXISTS idx_orders_asset_class ON orders(asset_class);
CREATE INDEX IF NOT EXISTS idx_fills_asset_class ON fills(asset_class);
CREATE INDEX IF NOT EXISTS idx_position_history_asset_class ON position_history(asset_class);
CREATE INDEX IF NOT EXISTS idx_audit_events_timestamp ON audit_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_audit_events_event_type ON audit_events(event_type);
CREATE INDEX IF NOT EXISTS idx_reconciliation_events_timestamp ON reconciliation_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_halt_events_timestamp ON halt_events(timestamp);
