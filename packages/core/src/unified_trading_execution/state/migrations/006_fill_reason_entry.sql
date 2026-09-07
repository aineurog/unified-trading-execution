-- 006: record why a fill happened and whether it opened/closed exposure.
-- Backward-compatible: existing rows keep NULL for both new columns.
ALTER TABLE fills ADD COLUMN reason TEXT;
ALTER TABLE fills ADD COLUMN entry TEXT;
