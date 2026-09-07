-- 007: current-state halt persistence (Section 6.4).
-- Mirrors the in-memory HaltStateMachine so active halts survive a restart.
-- One row per active halt: scope='account' uses empty symbol/asset_class,
-- scope='instrument' carries the serialised instrument identity.
CREATE TABLE IF NOT EXISTS halts (
    scope            TEXT NOT NULL,
    symbol           TEXT NOT NULL DEFAULT '',
    quote_currency   TEXT,
    asset_class      TEXT NOT NULL DEFAULT '',
    exchange         TEXT,
    currency         TEXT,
    expiry           TEXT,
    strike           TEXT,
    option_right     TEXT,
    multiplier       INTEGER,
    platform_symbol  TEXT,
    reason           TEXT NOT NULL,
    detail           TEXT NOT NULL,
    entered_at       TEXT NOT NULL,
    PRIMARY KEY (scope, symbol, asset_class)
);
