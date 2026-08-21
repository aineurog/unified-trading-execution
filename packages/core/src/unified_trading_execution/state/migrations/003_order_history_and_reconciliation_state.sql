-- 003: order_history (append-only lifecycle snapshots) and
-- reconciliation_state (persisted "clean through" watermark).
-- Applied by the migration runner in store.py on initialize().
-- Runs after 002_adapter_config.sql.

-- order_history preserves every OrderRecord snapshot, including terminal
-- transitions and orders later removed from the `orders` current-state table
-- by reconciliation orphan resolution. `orders` keeps the latest snapshot per
-- client_order_id; `order_history` keeps the full append-only lifecycle.
CREATE TABLE IF NOT EXISTS order_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id     TEXT NOT NULL,
    symbol              TEXT NOT NULL,
    quote_currency      TEXT,
    asset_class         TEXT NOT NULL,
    exchange            TEXT,
    currency            TEXT,
    expiry              TEXT,
    strike              TEXT,
    option_right        TEXT,
    multiplier          INTEGER,
    platform_symbol     TEXT,
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
    updated_at          TEXT NOT NULL,
    recorded_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_history_client_order_id ON order_history(client_order_id);
CREATE INDEX IF NOT EXISTS idx_order_history_recorded_at ON order_history(recorded_at);

-- reconciliation_state is a singleton key/value table holding the
-- `last_clean_through` watermark: the timestamp up to which the last
-- reconciliation pass verified everything clean. Reconciliation only compares
-- fills newer than this watermark, and advances it only on a clean pass.
CREATE TABLE IF NOT EXISTS reconciliation_state (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
