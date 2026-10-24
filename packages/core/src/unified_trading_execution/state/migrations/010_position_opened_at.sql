-- 010: persist the venue-reported position open time.
--
-- A trailing state must be able to tell a genuine reopen of a stable leg id
-- (Bybit positionIdx, Hyperliquid {coin}:oneWay) from the position the state
-- was attached to.  ``Position.opened_at`` carries the venue open time when
-- the adapter can supply it.  NULLable: several venues do not report one.
--
-- The column is only added to the live mirror; positions are current state,
-- not history, so there is no lifecycle log to widen.

ALTER TABLE positions ADD COLUMN opened_at TEXT;
