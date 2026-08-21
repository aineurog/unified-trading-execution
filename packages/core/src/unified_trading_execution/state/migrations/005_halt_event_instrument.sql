-- 005: halt_events stores the full instrument identity, not just symbol +
-- asset_class. Instrument now validates quote_currency for pairs and
-- expiry/strike/option_right/multiplier for futures/options, so a halt
-- event must round-trip those fields to reconstruct a valid Instrument.
-- Columns mirror the positions/order_history instrument columns.
ALTER TABLE halt_events ADD COLUMN quote_currency TEXT;
ALTER TABLE halt_events ADD COLUMN exchange TEXT;
ALTER TABLE halt_events ADD COLUMN currency TEXT;
ALTER TABLE halt_events ADD COLUMN expiry TEXT;
ALTER TABLE halt_events ADD COLUMN strike TEXT;
ALTER TABLE halt_events ADD COLUMN option_right TEXT;
ALTER TABLE halt_events ADD COLUMN multiplier INTEGER;
ALTER TABLE halt_events ADD COLUMN platform_symbol TEXT;
