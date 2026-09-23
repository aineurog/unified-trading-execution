"""Hyperliquid WS/REST message translation into unified core types.

Pure translation layer between Hyperliquid's push streams (``user``
fills/funding/liquidation, ``orderUpdates``, ``userFills``; snapshots
``allMids``/``l2Book``/``trades``) and REST reads (``clearinghouseState``,
``spotClearinghouseState``, ``openOrders`` + ``frontendOpenOrders``,
``userFills``/``userFillsByTime``, ``historicalOrders``) and the core types
carried by unified events.  No SDK imports and no I/O here.
"""

from __future__ import annotations

from typing import Any

from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position


def translate_fill(
    entry: dict[str, Any], *, instrument: Instrument, client_order_id: str
) -> FillRecord:
    """Build a ``FillRecord`` from one ``userFills`` entry.

    Maps price / size / time / hash to platform id, venue order id to the
    client id via the cloid index, ``fee`` (signed string — negative means
    maker rebate, sign preserved; zero-vs-missing stays distinct),
    ``feeToken``, ``builderFee`` passthrough and ``closedPnl`` diagnostics.
    The ``dir`` field maps to entry/reason (``Open Long``/``Open Short`` →
    IN with no reason; ``Close Long``/``Close Short`` → OUT with no reason
    unless TP/SL attribution is known).  Venue ``normalTpsl`` children carry
    no cloid of ours — they are keyed ``hl-tpsl-<oid>`` with TAKE_PROFIT /
    STOP_LOSS plus OUT.
    """
    raise NotImplementedError


def translate_position(entry: dict[str, Any], *, instrument: Instrument) -> Position:
    """Build a ``Position`` from one ``clearinghouseState`` leg.

    Asserts ``type == "oneWay"`` (no hedge legs can exist — no dual-leg
    bookkeeping, no net aggregation).  ``quantity`` is the signed ``szi``,
    entry is ``entryPx``, plus ``liquidationPx``, ``marginUsed`` and
    ``unrealizedPnl`` diagnostics.  ``position_id`` is ``"<coin>:oneWay"`` —
    unique per coin by construction.  Zero-size legs are skipped by the
    caller.
    """
    raise NotImplementedError


def translate_balance(entry: dict[str, Any], *, currency: str) -> Balance:
    """Build a ``Balance`` from one ``spotClearinghouseState`` row.

    ``total`` maps to total, ``hold`` to used, ``free = total - hold``; the
    core invariant ``free + used == total`` is enforced at construction.  No
    FX translation (native rows only; USDC is the quote everywhere).
    ``marginSummary`` plus ``withdrawable`` feed the USDC-row diagnostics.
    """
    raise NotImplementedError


def translate_order_entry(entry: dict[str, Any], *, instrument: Instrument) -> OrderRecord:
    """Build an ``OrderRecord`` from an open-order / status entry.

    Merges the ``openOrders`` + ``frontendOpenOrders`` shapes (the latter
    carries ``isPositionTpsl``, ``isTrigger`` and ``origSz``), keyed by
    cloid with a venue-oid fallback for venue-created children.
    """
    raise NotImplementedError


def translate_ticker(mid: str | None, *, best_bid: str | None, best_ask: str | None) -> Ticker:
    """Build a ``Ticker`` from an ``allMids`` mid plus ``l2Book`` top bid/ask.

    An empty book surfaces as ``None`` at the adapter (no live quote), not
    here.
    """
    raise NotImplementedError
