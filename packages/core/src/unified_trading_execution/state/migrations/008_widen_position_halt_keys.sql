-- 008: widen position/halt identity keys to include quote_currency.
--
-- A position's identity is (instrument, position_id), but the persisted key was
-- (symbol, asset_class, position_id).  Two instruments that share a base coin
-- but differ in quote — e.g. BTCUSDT (linear) and BTCUSD (inverse), both with
-- symbol "BTC" and asset_class "FUTURES" — collided on the same row, so one
-- position silently overwrote the other.  quote_currency is the discriminator
-- for the v1 domain (spot + linear/inverse perpetuals), so it is added to the
-- key.
--
-- SQLite cannot ALTER a PRIMARY KEY in place, so both current-state tables are
-- rebuilt with the widened key.  There are no foreign-key references into
-- positions or halts, so the drop/rename is safe.  quote_currency stays NULLable
-- (asset classes outside v1 may leave it unset); SQLite treats NULLs in a UNIQUE
-- key as distinct, which preserves the pre-v1 behaviour for those rows rather
-- than introducing a new collision.

CREATE TABLE positions_new (
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
    position_id         TEXT NOT NULL,
    quantity            TEXT NOT NULL,
    average_entry_price TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    PRIMARY KEY (symbol, quote_currency, asset_class, position_id)
);
INSERT INTO positions_new SELECT * FROM positions;
DROP TABLE positions;
ALTER TABLE positions_new RENAME TO positions;

CREATE TABLE halts_new (
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
    PRIMARY KEY (scope, symbol, quote_currency, asset_class)
);
INSERT INTO halts_new SELECT * FROM halts;
-- Normalise account-scoped halts (which the old schema stored with NULL
-- quote_currency) to the empty-string sentinel used for account rows, so a
-- pre-migration account halt and a post-migration re-persist do not diverge.
-- Scoped to account rows only: an instrument-scoped halt may legitimately
-- carry NULL quote_currency (asset classes outside v1), and NULL vs '' must
-- round-trip distinctly or the restored instrument identity would change.
UPDATE halts_new SET quote_currency = '' WHERE scope = 'account' AND quote_currency IS NULL;
DROP TABLE halts;
ALTER TABLE halts_new RENAME TO halts;
