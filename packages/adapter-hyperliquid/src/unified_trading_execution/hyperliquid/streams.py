"""Hyperliquid WS/REST message translation into unified core types.

Pure translation layer between Hyperliquid's push streams (``userEvents``
fills/funding/liquidation, ``orderUpdates``, ``userFills`` snapshots +
streaming; market snapshots ``allMids``/``l2Book``) and REST reads
(``clearinghouseState``, ``spotClearinghouseState``, ``openOrders`` +
``frontendOpenOrders``, ``userFills``/``userFillsByTime``,
``historicalOrders``) and the core types carried by unified events.  No SDK
imports and no I/O here — the adapter resolves each coin to a canonical
``Instrument`` and hands it in.  The same translators serve the WS handlers
and the REST snapshot reads so live mirror and snapshots can never disagree.

Wire facts (subscriptions reference + perpetuals/spot info references):
fills carry ``coin/px/sz/side(A|B)/time/startPosition/dir/closedPnl/hash/
oid/crossed/fee(negative = rebate)/tid/feeToken/builderFee?``; ``dir`` is a
display string (``Open Long``/``Open Short``/``Close Long``/``Close Short``,
spot ``Buy``/``Sell``); positions arrive as ``{type: "oneWay", position:
{coin, szi (signed), entryPx, leverage{type, value}, liquidationPx,
marginUsed, ...}}`` with no per-leg timestamp; spot balances as
``{coin, token, hold, total, entryNtl}`` with no timestamp; orders as the
thin ``WsBasicOrder`` (``coin/side/limitPx/sz/oid/timestamp/origSz/cloid?``)
or the richer frontend shape (adds ``orderType/isTrigger/triggerPx/
reduceOnly/isPositionTpsl``) — neither carries TP/SL legs (children are
separate orders).  Streaming user endpoints open with ``isSnapshot: true``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.hyperliquid.orders import map_order_status
from unified_trading_execution.types.enums import (
    FillEntry,
    FillReason,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position

_SIDE_TO_SIDE: dict[str, OrderSide] = {
    "B": OrderSide.BUY,  # bid-side
    "A": OrderSide.SELL,  # ask-side
}

_TIF_MAP: dict[str, TimeInForce] = {
    "Gtc": TimeInForce.GTC,
    "Ioc": TimeInForce.IOC,
    # Alo (post-only) has no core expression; a resting post-only order
    # behaves as GTC in the mirror (a match-cancel arrives as its own event).
    "Alo": TimeInForce.GTC,
}

# orderType spellings observed on the venue (frontend display values).
_TYPE_MAP: dict[str, OrderType] = {
    "Limit": OrderType.LIMIT,
    "Market": OrderType.MARKET,
}

_EMPTY: frozenset[Any] = frozenset({None, ""})


def _required_string(entry: dict[str, Any], field: str) -> str:
    raw = entry.get(field)
    if raw is None or raw == "":
        raise PlatformError(f"Hyperliquid stream message is missing required field {field!r}")
    return str(raw)


def _decimal(raw: object, field: str) -> Decimal:
    if raw is None or raw == "":
        raise PlatformError(f"Hyperliquid stream message is missing required field {field!r}")
    return Decimal(str(raw))


def _optional_decimal(raw: object) -> Decimal | None:
    if raw is None or raw == "" or str(raw) == "0":
        return None
    return Decimal(str(raw))


def _parse_ms(raw: object, field: str) -> datetime:
    if raw is None or raw == "":
        raise PlatformError(f"Hyperliquid stream message is missing timestamp {field!r}")
    ms = int(str(raw))
    seconds, millis = divmod(ms, 1000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=millis * 1000)


def translate_fill(
    entry: dict[str, Any], *, instrument: Instrument, client_order_id: str
) -> FillRecord:
    """Build a ``FillRecord`` from one fill entry (WS or REST shape, shared).

    ``platform_fill_id`` is ``"<hash>:<tid>"`` — the L1 hash alone is not
    unique per fill (one transaction can carry several), while ``tid``
    needs its block/coin context; the pair is unique.  ``fee`` keeps its
    sign (negative = maker rebate); zero-vs-missing stays distinct.
    ``dir`` maps to entry/reason (``Open *`` → IN, ``Close *`` → OUT, spot
    ``Buy`` → IN / ``Sell`` → OUT, all with no reason unless TP/SL
    attribution is known — venue ``normalTpsl`` children keyed
    ``hl-tpsl-<oid>`` arrive with TAKE_PROFIT/STOP_LOSS + OUT from the
    caller).  Unrecognized ``dir`` values yield no entry/reason rather than
    a guess; ``closedPnl`` has no core field and is dropped.
    """
    direction = entry.get("dir")
    entry_side: FillEntry | None = None
    reason: FillReason | None = None
    if direction in ("Open Long", "Open Short"):
        entry_side = FillEntry.IN
    elif direction in ("Close Long", "Close Short"):
        entry_side = FillEntry.OUT
    elif direction == "Buy":
        entry_side = FillEntry.IN
    elif direction == "Sell":
        entry_side = FillEntry.OUT
    return FillRecord(
        client_order_id=client_order_id,
        platform_fill_id=f"{_required_string(entry, 'hash')}:{_required_string(entry, 'tid')}",
        instrument=instrument,
        fill_quantity=_decimal(entry.get("sz"), "sz"),
        fill_price=_decimal(entry.get("px"), "px"),
        fill_timestamp=_parse_ms(entry.get("time"), "time"),
        fee_currency=entry.get("feeToken") if entry.get("feeToken") not in _EMPTY else None,
        fee_amount=_parse_fee(entry.get("fee")),
        correlation_id=client_order_id,
        position_id=None,
        reason=reason,
        entry=entry_side,
    )


def _parse_fee(raw: object) -> Decimal | None:
    """Parse the ``fee`` field — zero fee is distinct from missing data."""
    if raw is None or raw == "":
        return None
    return Decimal(str(raw))


def translate_position(
    entry: dict[str, Any], *, instrument: Instrument, timestamp: datetime
) -> Position:
    """Build a ``Position`` from one ``clearinghouseState`` leg.

    Asserts ``type == "oneWay"`` (no hedge legs can exist).  ``quantity``
    is the signed ``szi`` (positive = long, negative = short — the core
    convention); entry is ``entryPx``; ``liquidationPx``/``marginUsed``/
    ``unrealizedPnl`` ride diagnostics.  ``position_id`` is
    ``"<coin>:oneWay"`` — unique per coin by construction.  Legs carry no
    timestamp, so the caller passes the state's time.  Zero-size legs are
    skipped by the caller not here (the WS emits them on close).
    """
    if entry.get("type") != "oneWay":
        raise PlatformError(f"Unexpected position type {entry.get('type')!r} (expected 'oneWay')")
    leg = entry.get("position")
    if not isinstance(leg, dict):
        raise PlatformError("Hyperliquid position entry is missing its 'position' leg")
    coin = _required_string(leg, "coin")
    return Position(
        instrument=instrument,
        quantity=_decimal(leg.get("szi"), "szi"),
        average_entry_price=_decimal(leg.get("entryPx") or "0", "entryPx"),
        updated_at=timestamp,
        position_id=f"{coin}:oneWay",
    )


def translate_balance(entry: dict[str, Any], *, currency: str, timestamp: datetime) -> Balance:
    """Build a ``Balance`` from one spot-balance row.

    ``total`` → total, ``hold`` → used, ``free = total - hold``; the core
    invariant ``free + used == total`` is enforced at construction (free is
    clamped at zero so a transient over-hold never inverts it).  No FX
    translation (native rows only; USDC is the quote everywhere).  Rows
    carry no timestamp, so the caller passes the state's time.
    """
    total = _decimal(entry.get("total"), "total")
    hold_raw = entry.get("hold")
    used = Decimal(str(hold_raw)) if hold_raw not in _EMPTY else Decimal("0")
    free = total - used
    if free < 0:
        free = Decimal("0")
    return Balance(
        currency=currency,
        free=free,
        used=used,
        total=free + used,
        updated_at=timestamp,
    )


def _translate_side(entry: dict[str, Any]) -> OrderSide:
    raw = entry.get("side")
    side = _SIDE_TO_SIDE.get(raw) if isinstance(raw, str) else None
    if side is None:
        raise PlatformError(f"Unknown order side {raw!r}")
    return side


def _translate_order_type(entry: dict[str, Any]) -> OrderType:
    """Resolve the order type across the thin and rich entry shapes.

    Explicit ``orderType`` first (``Stop``/``Take Profit`` prefixed display
    values split into STOP/STOP_LIMIT by limit-vs-market suffix); then the
    trigger markers (``isTrigger`` or a nonzero ``triggerPx``); finally the
    resting default — the venue only rests limit and trigger types and a
    market IOC never rests, so a priced resting order with no trigger
    marker is a limit.
    """
    raw_type = entry.get("orderType")
    if isinstance(raw_type, str) and raw_type:
        if raw_type.startswith(("Stop", "Take Profit")):
            return OrderType.STOP_LIMIT if raw_type.endswith("Limit") else OrderType.STOP
        mapped = _TYPE_MAP.get(raw_type)
        if mapped is not None:
            return mapped
        raise PlatformError(f"Unknown order type {raw_type!r}")
    trigger_px = _optional_decimal(entry.get("triggerPx"))
    if entry.get("isTrigger") or trigger_px is not None:
        limit_px = _optional_decimal(entry.get("limitPx"))
        return OrderType.STOP_LIMIT if limit_px is not None else OrderType.STOP
    return OrderType.LIMIT


def translate_order_entry(entry: dict[str, Any], *, instrument: Instrument) -> OrderRecord:
    """Build an ``OrderRecord`` from an open-order / update entry.

    Handles the thin ``WsBasicOrder`` shape and the richer frontend shape:
    ``quantity`` prefers ``origSz`` (requested) over ``sz`` (remaining);
    ``filled_quantity`` derives as requested-minus-remaining when both are
    known; ``client_order_id`` falls back to ``""`` for venue-created
    children (keyed by oid upstream); status reads ``status`` when present
    and defaults to OPEN (open-order endpoints only list working orders);
    ``time_in_force`` defaults to GTC when omitted; TP/SL legs are separate
    orders so the record carries none.
    """
    side = _translate_side(entry)
    order_type = _translate_order_type(entry)

    tif_raw = entry.get("tif") or "Gtc"
    time_in_force = _TIF_MAP.get(tif_raw)
    if time_in_force is None:
        raise PlatformError(f"Unknown timeInForce {tif_raw!r}")

    requested = _optional_decimal(entry.get("origSz"))
    remaining = _optional_decimal(entry.get("sz"))
    if requested is not None:
        quantity = requested
        filled = requested - remaining if remaining is not None else Decimal("0")
    elif remaining is not None:
        quantity = remaining
        filled = Decimal("0")
    else:
        raise PlatformError("Hyperliquid order entry carries no size (sz/origSz)")

    status_raw = entry.get("status")
    status = (
        map_order_status(status_raw)
        if isinstance(status_raw, str) and status_raw
        else OrderStatus.OPEN
    )

    created_at = _parse_ms(entry.get("timestamp"), "timestamp")
    updated_raw = entry.get("statusTimestamp")
    updated_at = (
        _parse_ms(updated_raw, "statusTimestamp") if updated_raw not in _EMPTY else created_at
    )

    platform_order_id = entry.get("oid")
    if platform_order_id is None or platform_order_id == "":
        raise PlatformError("Hyperliquid order entry is missing oid")
    client_order_id = entry.get("cloid") or ""

    return OrderRecord(
        instrument=instrument,
        order_type=order_type,
        side=side,
        quantity=quantity,
        time_in_force=time_in_force,
        client_order_id=str(client_order_id),
        price=_optional_decimal(entry.get("limitPx")),
        stop_price=_optional_decimal(entry.get("triggerPx")),
        reduce_only=bool(entry.get("reduceOnly")),
        client_tag=None,
        take_profit=None,
        stop_loss=None,
        platform_order_id=str(platform_order_id),
        status=status,
        filled_quantity=filled if filled >= 0 else Decimal("0"),
        average_fill_price=None,
        correlation_id=str(client_order_id),
        created_at=created_at,
        updated_at=updated_at,
    )


def translate_ticker(
    mid: str | None,
    *,
    best_bid: str | None,
    best_ask: str | None,
    mark: str | None = None,
) -> Ticker:
    """Build a ``Ticker`` from an ``allMids`` mid plus book top and mark.

    Empty books and missing marks surface as ``None`` fields (never
    zeroes); the adapter returns ``None`` instead of a ``Ticker`` when
    nothing at all is quoted.
    """
    return Ticker(
        bid=_optional_decimal(best_bid),
        ask=_optional_decimal(best_ask),
        last=_optional_decimal(mid),
        mark=_optional_decimal(mark),
    )
