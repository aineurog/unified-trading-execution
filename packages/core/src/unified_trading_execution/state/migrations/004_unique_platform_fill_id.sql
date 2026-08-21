-- 004: make platform fills idempotent.
-- Keep the newest existing row before adding the uniqueness guarantee.
DELETE FROM fills
WHERE rowid NOT IN (
    SELECT MAX(rowid)
    FROM fills
    GROUP BY platform_fill_id
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_platform_fill_id
    ON fills(platform_fill_id);
