-- 009: persist GTD order expiry so an expired order the platform drops
-- reconciles to EXPIRED instead of silently vanishing.
--
-- ``OrderRecord`` gains ``expire_at`` (the GTD good-til date carried by
-- ``UnifiedOrder``).  Until now that date was dropped at the adapter
-- read-back boundary, so when a GTD order lapsed and the venue removed it,
-- reconciliation saw a local-only orphan with no way to distinguish
-- "expired" from "vanished" and deleted it, leaving the order history as
-- OPEN-then-gone.  With the date persisted, a past-expiry orphan becomes a
-- terminal EXPIRED transition.
--
-- Both the live mirror (``orders``) and the append-only lifecycle log
-- (``order_history``) gain the column.  It is NULLable: non-GTD orders and
-- pre-009 rows legitimately carry no expiry.

ALTER TABLE orders ADD COLUMN expire_at TEXT;
ALTER TABLE order_history ADD COLUMN expire_at TEXT;
