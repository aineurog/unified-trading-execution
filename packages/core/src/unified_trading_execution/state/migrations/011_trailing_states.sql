-- 011: trailing states.
--
-- One row per trailing position leg.  Terminal rows are kept as history and are
-- never deleted, so a new attach can reuse the position without losing the old
-- record.  Decimals are stored as text, enums by value, booleans as integers,
-- matching the positions and orders tables.
--
-- Identity includes quote_currency so BTCUSDT (linear) and BTCUSD (inverse),
-- which share the symbol "BTC", cannot collide (the bug migration 008 fixed).

CREATE TABLE IF NOT EXISTS trailing_states (
    trailing_id              TEXT PRIMARY KEY,
    symbol                   TEXT NOT NULL,
    quote_currency           TEXT,
    asset_class              TEXT NOT NULL,
    exchange                 TEXT,
    currency                 TEXT,
    expiry                   TEXT,
    strike                   TEXT,
    option_right             TEXT,
    multiplier               INTEGER,
    platform_symbol          TEXT,
    position_id              TEXT NOT NULL,
    position_opened_at       TEXT,
    side                     TEXT NOT NULL,
    mode                     TEXT NOT NULL,
    routing_reason           TEXT,
    phase                    TEXT NOT NULL,
    offset_type              TEXT NOT NULL,
    "offset"                 TEXT NOT NULL,
    basis                    TEXT,
    resolved_basis           TEXT,
    protection_trigger_basis TEXT,
    activation               TEXT NOT NULL,
    activation_price         TEXT,
    activation_profit_percent TEXT,
    initial_stop_type        TEXT,
    initial_stop_value       TEXT,
    take_profit_type         TEXT,
    take_profit_value        TEXT,
    step                     TEXT,
    atr_period               INTEGER,
    atr_timeframe            TEXT,
    atr_value                TEXT,
    atr_as_of                TEXT,
    entry_price              TEXT NOT NULL,
    quantity                 TEXT NOT NULL,
    current_stop             TEXT,
    current_take_profit      TEXT,
    stop_owned               INTEGER NOT NULL DEFAULT 0,
    take_profit_owned        INTEGER NOT NULL DEFAULT 0,
    previous_stop_loss       TEXT,
    previous_take_profit     TEXT,
    best_basis               TEXT,
    resolved_activation_price TEXT,
    activated                INTEGER NOT NULL,
    native_id                TEXT,
    last_reprice_at          TEXT,
    reprice_count            INTEGER NOT NULL DEFAULT 0,
    last_error               TEXT,
    failure_count            INTEGER NOT NULL DEFAULT 0,
    created_at               TEXT NOT NULL,
    updated_at               TEXT NOT NULL
);

-- At most one non-terminal state per position leg.  Terminal states must not
-- block a new attach, so this is a partial index rather than a plain unique
-- index.  quote_currency is nullable; SQLite treats NULLs as distinct, which
-- matches the positions table.
CREATE UNIQUE INDEX IF NOT EXISTS idx_trailing_active_position
ON trailing_states(symbol, quote_currency, asset_class, position_id)
WHERE phase IN ('arming', 'armed', 'triggered', 'degraded');

CREATE INDEX IF NOT EXISTS idx_trailing_states_phase
ON trailing_states(phase);

CREATE INDEX IF NOT EXISTS idx_trailing_states_instrument
ON trailing_states(symbol, quote_currency, asset_class);
