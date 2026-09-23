"""UnifiedOrder ↔ Hyperliquid action payload translation.

Pure translation layer between the engine's canonical order model and
Hyperliquid's order/modify/cancel actions.  No SDK imports and no I/O here —
the adapter resolves the asset id and forwards the returned payload.
"""

from __future__ import annotations

from typing import Any

from unified_trading_execution.types.enums import OrderStatus
from unified_trading_execution.types.order import OrderModification, OrderResult, UnifiedOrder


def build_place_order_action(
    order: UnifiedOrder,
    *,
    asset_id: int,
    client_order_id: str,
) -> dict[str, Any]:
    """Translate a validated ``UnifiedOrder`` into an order action.

    MARKET → aggressive limit IOC at touch ± slippage band (no native market
    TIF; ``FrontendMarket`` is display-only, never submitted).  LIMIT →
    ``t.limit`` with tif ``Gtc`` | ``Ioc`` | ``Alo`` (``Alo`` only via an
    explicit post-only flag, never silently — Alo-cancel differs from
    IOC-cancel).  STOP/STOP_LIMIT → ``t.trigger`` with ``isMarket``,
    ``triggerPx`` and ``tpsl: "sl"`` (plus ``p`` for the limit leg).
    ``cloid`` is always set from ``client_order_id`` (128-bit hex; UUID7 hex
    fits exactly) enabling ``cancelByCloid``, ``orderStatus`` and idempotent
    retry.  ``reduce_only`` sets the ``r`` flag (derivatives only — spot +
    reduce-only raises).  TP/SL attachments use ``trigger`` with
    ``tpsl: "tp" | "sl"`` plus ``grouping`` (``normalTpsl`` OCO pair or
    ``positionTpsl`` whole-position); non-reduce-only TP/SL is rejected
    client-side first and the trigger reference is mark price.  GTD/DAY/FOK
    and chase/scale/TWAP/trailing raise ``UnsupportedOrderTypeError`` by name.
    """
    raise NotImplementedError


def build_modify_action(
    modification: OrderModification,
    *,
    asset_id: int,
) -> dict[str, Any]:
    """Translate an ``OrderModification`` into a modify action.

    Modify by oid or cloid (prefer cloid — stable across restarts); same
    order schema as place.  Replacement semantics are not silently
    downgraded — a replacement that must be non-trigger ALO-or-dead
    surfaces a rejection otherwise.
    """
    raise NotImplementedError


def build_cancel_action(
    client_order_id: str,
    *,
    asset_id: int,
) -> dict[str, Any]:
    """Translate a client order id into a ``cancelByCloid`` action.

    The cloid path is preferred end-to-end.  The ``fast`` flag is omitted
    when false and never true for triggers.
    """
    raise NotImplementedError


def map_order_status(status: str) -> OrderStatus:
    """Map a native order status to the unified ``OrderStatus``.

    Covers ``open`` / ``filled`` / ``canceled`` / ``triggered`` /
    ``rejected`` / ``marginCanceled`` / ``siblingFilledCanceled`` and every
    ``*Rejected`` variant (surfaced as ``InvalidOrderError`` upstream).
    Raises ``PlatformError`` for an unrecognised status — a new status must
    never be silently misrepresented.
    """
    raise NotImplementedError


def parse_order_result(entry: dict[str, Any], client_order_id: str) -> OrderResult:
    """Build an ``OrderResult`` from an action response / status entry.

    Response ``statuses`` handling: ``resting`` with oid → OPEN/PENDING,
    ``filled`` with total size and average price → FILLED/PARTIAL (filled
    vs requested), ``triggered`` → OPEN (working trigger), ``error`` →
    mapped exception.  A batch pre-validation failure (single error for the
    whole batch) raises on the first item — never partial-apply.
    """
    raise NotImplementedError
