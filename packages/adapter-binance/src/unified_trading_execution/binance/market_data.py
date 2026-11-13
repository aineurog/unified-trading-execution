"""Binance market-data request routing and response translation.

Public market-data endpoints are unsigned GETs. Spot, USDⓈ-M, and COIN-M
each publish a 24-hour ticker, a best-bid/offer ticker, and — for Futures —
a premium-index mark price. COIN-M answers symbol-scoped queries with
single-element lists while Spot and USDⓈ-M answer with objects, so payload
normalization lives here rather than in the adapter.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TypeAlias

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.errors import PlatformError
from unified_trading_execution.trailing.types import CandleTimeframe
from unified_trading_execution.types.market_data import Candle

logger = logging.getLogger(__name__)

_Metadata: TypeAlias = dict[str, object]

# Every core candle timeframe maps to a Binance kline interval string that is
# published identically by Spot, USDⓈ-M, and COIN-M (verified against the
# pinned SDK and Demo exchange data, including the 8h and 12h bars).
_KLINE_INTERVALS: dict[CandleTimeframe, str] = {
    CandleTimeframe.M1: "1m",
    CandleTimeframe.M5: "5m",
    CandleTimeframe.M15: "15m",
    CandleTimeframe.M30: "30m",
    CandleTimeframe.H1: "1h",
    CandleTimeframe.H4: "4h",
    CandleTimeframe.H8: "8h",
    CandleTimeframe.H12: "12h",
    CandleTimeframe.D1: "1d",
}

_CANDLE_SECONDS: dict[CandleTimeframe, int] = {
    CandleTimeframe.M1: 60,
    CandleTimeframe.M5: 300,
    CandleTimeframe.M15: 900,
    CandleTimeframe.M30: 1800,
    CandleTimeframe.H1: 3600,
    CandleTimeframe.H4: 14400,
    CandleTimeframe.H8: 28800,
    CandleTimeframe.H12: 43200,
    CandleTimeframe.D1: 86400,
}

# Binance caps the kline ``limit`` parameter at 1000 per request.
MAX_KLINE_LIMIT = 1000

# ``futures_*`` method names on the pinned AsyncClient per product. The
# latest-price ticker supplies ``last``; the book ticker supplies ``bidPrice``
# / ``askPrice`` (Futures price tickers carry no book fields); the premium
# index supplies ``markPrice``. Spot reads all three price fields from its
# 24-hour ticker in a single call, so it uses ``get_ticker`` for ``last``.
_LAST_PRICE_METHODS: dict[BinanceProduct, str] = {
    BinanceProduct.SPOT: "get_ticker",
    BinanceProduct.USD_M: "futures_symbol_ticker",
    BinanceProduct.COIN_M: "futures_coin_symbol_ticker",
}
# Response field carrying the latest trade price per endpoint: the 24h ticker
# calls it ``lastPrice``; the dedicated price tickers call it ``price``.
_LAST_PRICE_FIELDS: dict[BinanceProduct, str] = {
    BinanceProduct.SPOT: "lastPrice",
    BinanceProduct.USD_M: "price",
    BinanceProduct.COIN_M: "price",
}
_BOOK_TICKER_METHODS: dict[BinanceProduct, str] = {
    BinanceProduct.SPOT: "get_orderbook_ticker",
    BinanceProduct.USD_M: "futures_orderbook_ticker",
    BinanceProduct.COIN_M: "futures_coin_orderbook_ticker",
}
_MARK_PRICE_METHODS: dict[BinanceProduct, str] = {
    BinanceProduct.USD_M: "futures_mark_price",
    BinanceProduct.COIN_M: "futures_coin_mark_price",
}
_KLINES_METHODS: dict[BinanceProduct, str] = {
    BinanceProduct.SPOT: "get_klines",
    BinanceProduct.USD_M: "futures_klines",
    BinanceProduct.COIN_M: "futures_coin_klines",
}

# contractType values accepted by the COIN-M klines endpoint. Exchange-info
# rows may carry a "*_DELIVERING" suffix while the contract is inside its
# delivery window; the klines API addresses the contract slot without it.
_COIN_M_KLINE_CONTRACT_TYPES = frozenset(
    {
        "PERPETUAL",
        "CURRENT_WEEK",
        "NEXT_WEEK",
        "CURRENT_MONTH",
        "NEXT_MONTH",
        "CURRENT_QUARTER",
        "NEXT_QUARTER",
    }
)


def kline_interval(timeframe: CandleTimeframe) -> str | None:
    """Return the Binance interval string for a core candle timeframe."""
    return _KLINE_INTERVALS.get(timeframe)


def supported_timeframes() -> frozenset[CandleTimeframe]:
    """Return every core timeframe Binance klines can serve."""
    return frozenset(_KLINE_INTERVALS)


def candle_seconds(timeframe: CandleTimeframe) -> int:
    """Return the duration of one candle bar for a core timeframe."""
    return _CANDLE_SECONDS[timeframe]


def last_price_method(product: BinanceProduct) -> str:
    return _LAST_PRICE_METHODS[product]


def last_price_field(product: BinanceProduct) -> str:
    return _LAST_PRICE_FIELDS[product]


def book_ticker_method(product: BinanceProduct) -> str:
    return _BOOK_TICKER_METHODS[product]


def mark_price_method(product: BinanceProduct) -> str | None:
    """Return the premium-index method for Futures, ``None`` for Spot."""
    return _MARK_PRICE_METHODS.get(product)


def klines_method(product: BinanceProduct) -> str:
    return _KLINES_METHODS[product]


def symbol_entry(
    response: object,
    *,
    symbol: str,
    product: BinanceProduct,
) -> _Metadata | None:
    """Normalize a dict-or-list venue payload to the requested symbol's row.

    Spot and USDⓈ-M symbol-scoped queries answer with an object; COIN-M
    answers with a single-element list. ``None`` means the venue returned no
    row for the symbol — a known symbol with no live quote, not an error.
    """
    if isinstance(response, dict):
        entry_symbol = response.get("symbol")
        if entry_symbol is not None and entry_symbol != symbol:
            raise PlatformError(
                f"Binance {product.value} market-data response symbol mismatch: "
                f"requested {symbol}, received {entry_symbol!r}",
                platform_error=response,
            )
        return response
    if isinstance(response, list):
        entries: list[_Metadata] = []
        for item in response:
            if not isinstance(item, dict):
                raise PlatformError(
                    f"Binance {product.value} market-data response contains a non-object entry",
                    platform_error=response,
                )
            entries.append(item)
        matches = [entry for entry in entries if entry.get("symbol") == symbol]
        if len(matches) > 1:
            raise PlatformError(
                f"Binance {product.value} returned duplicate market-data rows for {symbol}",
                platform_error=response,
            )
        return matches[0] if matches else None
    raise PlatformError(
        f"Binance {product.value} market-data response is not an object or list",
        platform_error=response,
    )


def optional_price(
    entry: _Metadata,
    key: str,
    *,
    symbol: str,
    product: BinanceProduct,
) -> Decimal | None:
    """Parse an optional decimal price field; ``None``/``""`` mean absent."""
    value = entry.get(key)
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PlatformError(
            f"Binance {product.value} ticker has invalid {key} for {symbol}: {value!r}"
        ) from exc
    if not parsed.is_finite() or parsed < 0:
        raise PlatformError(
            f"Binance {product.value} ticker has invalid {key} for {symbol}: {value!r}"
        )
    return parsed if parsed > 0 else None


def parse_kline_row(
    row: object,
    *,
    timeframe: CandleTimeframe,
    now: datetime,
) -> Candle | None:
    """Translate one Binance kline array row into a closed ``Candle``.

    Binance rows are ``[openTime, open, high, low, close, volume, closeTime,
    ...]`` with millisecond timestamps. The bar whose computed close is still
    in the future is forming and returns ``None``, as does a malformed row
    (logged, matching the cross-adapter "fewer candles" contract).
    """
    try:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            raise ValueError("kline row is not an array of at least 5 fields")
        open_ms = int(str(row[0]))
        open_time = datetime.fromtimestamp(open_ms / 1000, tz=UTC)
        close_time = open_time + timedelta(seconds=candle_seconds(timeframe))
        if close_time > now:
            return None
        return Candle(
            open_time=open_time,
            close_time=close_time,
            open=Decimal(str(row[1])),
            high=Decimal(str(row[2])),
            low=Decimal(str(row[3])),
            close=Decimal(str(row[4])),
        )
    except (IndexError, TypeError, ValueError, OverflowError, OSError, InvalidOperation):
        logger.warning("Skipping malformed Binance kline row: %s", row)
        return None


def coin_m_contract_type(entry: _Metadata, *, symbol: str) -> str:
    """Map an exchange-info ``contractType`` to the COIN-M klines parameter."""
    raw = entry.get("contractType")
    if not isinstance(raw, str) or not raw.strip():
        raise PlatformError(
            f"Binance COIN-M exchange-info row for {symbol} has no contractType",
            platform_error=entry,
        )
    normalized = raw.strip().replace(" ", "_")
    if normalized.endswith("_DELIVERING"):
        normalized = normalized[: -len("_DELIVERING")]
    if normalized not in _COIN_M_KLINE_CONTRACT_TYPES:
        raise PlatformError(
            f"Binance COIN-M contract type {raw!r} for {symbol} has no klines mapping",
            platform_error=entry,
        )
    return normalized
