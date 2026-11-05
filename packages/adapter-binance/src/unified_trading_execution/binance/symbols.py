"""Canonical instrument and Binance exchange-info translation."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import TypeAlias

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec

_DELIVERY_CONTRACT_TYPES = frozenset(
    {
        "CURRENT_WEEK",
        "NEXT_WEEK",
        "CURRENT_MONTH",
        "NEXT_MONTH",
        "CURRENT_QUARTER",
        "NEXT_QUARTER",
        "CURRENT_QUARTER_DELIVERING",
        "NEXT_QUARTER_DELIVERING",
        "PERPETUAL_DELIVERING",
    }
)
_Metadata: TypeAlias = dict[str, object]


def product_for_instrument(instrument: Instrument) -> BinanceProduct:
    """Resolve a supported Binance product from explicit canonical fields.

    Spot instruments use ``AssetClass.SPOT``. Futures must identify their
    settlement asset through ``currency``: quote-margined contracts use the
    quote asset, while COIN-M contracts use their base/margin asset and USD
    quote. Other futures identities are rejected rather than guessed.
    """
    if instrument.asset_class == AssetClass.SPOT:
        if instrument.expiry is not None:
            raise InvalidSymbolError("Binance Spot instruments cannot have an expiry")
        return BinanceProduct.SPOT

    if instrument.asset_class != AssetClass.FUTURES:
        raise InvalidSymbolError(
            f"Asset class {instrument.asset_class} is not supported by Binance"
        )
    if instrument.quote_currency is None:
        raise InvalidSymbolError("Binance Futures instruments require quote_currency")

    if instrument.currency == instrument.quote_currency:
        return BinanceProduct.USD_M
    if instrument.quote_currency == "USD" and instrument.currency == instrument.symbol:
        return BinanceProduct.COIN_M
    raise InvalidSymbolError(
        "Cannot resolve Binance Futures product: currency must equal quote_currency "
        "for USDⓈ-M, or equal symbol with USD quote for COIN-M"
    )


def to_binance_symbol(
    instrument: Instrument,
    product: BinanceProduct | None = None,
) -> str:
    """Return a candidate Binance symbol for an explicitly identified product.

    A supplied ``platform_symbol`` is authoritative. Otherwise, this builds
    Binance's conventional pair/contract spelling; callers resolving against
    exchange metadata must still confirm that the candidate is unique.
    """
    resolved_product = product_for_instrument(instrument)
    if product is not None and product != resolved_product:
        raise InvalidSymbolError(
            f"Instrument resolves to {resolved_product.value}, not {product.value}"
        )
    if instrument.platform_symbol is not None:
        if (
            not instrument.platform_symbol.strip()
            or instrument.platform_symbol != instrument.platform_symbol.strip()
        ):
            raise InvalidSymbolError(
                "platform_symbol must be non-empty without surrounding whitespace"
            )
        return instrument.platform_symbol

    quote = instrument.quote_currency
    if quote is None:
        raise InvalidSymbolError(
            f"Instrument {instrument.symbol} has no quote_currency for Binance"
        )
    pair = f"{instrument.symbol}{quote}"
    if instrument.expiry is not None:
        return f"{pair}_{instrument.expiry:%y%m%d}"
    if resolved_product != BinanceProduct.COIN_M:
        return pair
    return f"{pair}_PERP"


def from_binance_symbol(entry: _Metadata, product: BinanceProduct) -> Instrument:
    """Build a canonical instrument from one product's exchange-info row."""
    symbol = _required_string(entry, "symbol")
    base = _required_string(entry, "baseAsset")
    quote = _required_string(entry, "quoteAsset")

    if product == BinanceProduct.SPOT:
        return Instrument(
            symbol=base,
            quote_currency=quote,
            asset_class=AssetClass.SPOT,
            platform_symbol=symbol,
        )

    if product not in (BinanceProduct.USD_M, BinanceProduct.COIN_M):
        raise InvalidSymbolError(f"Unsupported Binance product {product!r}")

    contract_type = _required_string(entry, "contractType")
    # Delivering types arrive with a space ("CURRENT_QUARTER DELIVERING").
    normalized_type = contract_type.replace(" ", "_")
    if normalized_type == "PERPETUAL":
        expiry = None
    elif normalized_type in _DELIVERY_CONTRACT_TYPES:
        expiry = _delivery_date(entry, symbol)
    else:
        raise InvalidSymbolError(
            f"Unsupported Binance Futures contract type {contract_type!r} for {symbol}"
        )

    margin_asset = _required_string(entry, "marginAsset")
    if product == BinanceProduct.USD_M:
        if margin_asset != quote:
            raise InvalidSymbolError(
                f"USDⓈ-M symbol {symbol} has margin asset {margin_asset!r}, "
                f"expected quote asset {quote!r}"
            )
        # fapi exchange info omits contractSize; linear contracts are sized at 1.
        multiplier = 1
    else:
        if quote != "USD" or margin_asset != base:
            raise InvalidSymbolError(
                f"COIN-M symbol {symbol} must be USD-quoted and margined in its base asset"
            )
        multiplier = _contract_size(entry, symbol)

    return Instrument(
        symbol=base,
        quote_currency=quote,
        asset_class=AssetClass.FUTURES,
        currency=margin_asset,
        expiry=expiry,
        multiplier=multiplier,
        platform_symbol=symbol,
    )


def resolve_binance_symbol(
    instrument: Instrument,
    product: BinanceProduct,
    entries: list[object],
) -> tuple[str, _Metadata]:
    """Resolve exactly one matching symbol row from product exchange info."""
    if product_for_instrument(instrument) != product:
        raise InvalidSymbolError(f"Instrument does not belong to Binance product {product.value}")

    matches: list[tuple[str, _Metadata]] = []
    for raw_entry in entries:
        if not isinstance(raw_entry, dict):
            raise PlatformError("Malformed Binance exchange-info symbols entry")
        entry: _Metadata = raw_entry
        try:
            candidate = from_binance_symbol(entry, product)
        except InvalidSymbolError:
            continue

        if not _same_instrument(candidate, instrument):
            continue
        if (
            instrument.platform_symbol is not None
            and entry.get("symbol") != instrument.platform_symbol
        ):
            continue
        symbol = entry.get("symbol")
        if isinstance(symbol, str):
            matches.append((symbol, entry))

    if not matches:
        raise InvalidSymbolError(
            f"No Binance {product.value} symbol matches {instrument.symbol}/"
            f"{instrument.quote_currency}"
        )
    if len(matches) != 1:
        raise InvalidSymbolError(
            f"Binance instrument {instrument.symbol}/{instrument.quote_currency} "
            f"is ambiguous for {product.value}; set platform_symbol explicitly"
        )
    return matches[0]


def parse_instrument_spec(entry: _Metadata) -> InstrumentSpec:
    """Parse Binance PRICE_FILTER, LOT_SIZE, and notional filters exactly."""
    symbol = _required_string(entry, "symbol")
    filters_raw = entry.get("filters")
    if not isinstance(filters_raw, list):
        raise PlatformError(f"Malformed Binance filters for {symbol}")

    filters: dict[str, _Metadata] = {}
    for raw_filter in filters_raw:
        if not isinstance(raw_filter, dict):
            raise PlatformError(f"Malformed Binance filter for {symbol}")
        filter_type = raw_filter.get("filterType")
        if not isinstance(filter_type, str):
            raise PlatformError(f"Binance filter for {symbol} has no filterType")
        if filter_type in filters:
            raise PlatformError(f"Duplicate Binance {filter_type} filter for {symbol}")
        filters[filter_type] = raw_filter

    price_filter = _required_filter(filters, "PRICE_FILTER", symbol)
    lot_filter = _required_filter(filters, "LOT_SIZE", symbol)
    tick_size = _positive_decimal(price_filter, "tickSize", symbol)
    lot_size = _positive_decimal(lot_filter, "stepSize", symbol)
    min_qty = _non_negative_decimal(lot_filter, "minQty", symbol)
    max_qty = _positive_decimal(lot_filter, "maxQty", symbol)
    if min_qty > max_qty:
        raise PlatformError(f"Binance LOT_SIZE minQty exceeds maxQty for {symbol}")

    min_notionals: list[Decimal] = []
    for filter_type, field in (
        ("MIN_NOTIONAL", "minNotional"),
        ("NOTIONAL", "minNotional"),
    ):
        notional_filter = filters.get(filter_type)
        if notional_filter is not None:
            if field in notional_filter:
                min_notionals.append(_non_negative_decimal(notional_filter, field, symbol))
            elif filter_type == "MIN_NOTIONAL" and "notional" in notional_filter:
                min_notionals.append(_non_negative_decimal(notional_filter, "notional", symbol))
            else:
                raise PlatformError(
                    f"Binance {filter_type} filter has no minimum notional for {symbol}"
                )
    min_notional = max(min_notionals, default=Decimal(0))

    return InstrumentSpec(
        tick_size=tick_size,
        lot_size=lot_size,
        min_qty=min_qty,
        max_qty=max_qty,
        min_notional=min_notional,
        price_precision=_decimal_places(tick_size),
        qty_precision=_decimal_places(lot_size),
        max_leverage=None,
        min_stop_distance=None,
    )


def _same_instrument(left: Instrument, right: Instrument) -> bool:
    return (
        left.symbol == right.symbol
        and left.quote_currency == right.quote_currency
        and left.asset_class == right.asset_class
        and left.currency == right.currency
        and left.expiry == right.expiry
    )


def _delivery_date(entry: _Metadata, symbol: str) -> date:
    raw_value = entry.get("deliveryDate")
    try:
        if isinstance(raw_value, bool) or not isinstance(raw_value, (str, int)):
            raise ValueError
        milliseconds = int(raw_value)
        if milliseconds <= 0:
            raise ValueError
        return datetime.fromtimestamp(milliseconds / 1000, tz=UTC).date()
    except (OverflowError, OSError, TypeError, ValueError) as exc:
        raise InvalidSymbolError(
            f"Binance delivery contract {symbol} has invalid deliveryDate"
        ) from exc


def _contract_size(entry: _Metadata, symbol: str) -> int:
    raw_value = entry.get("contractSize")
    if isinstance(raw_value, bool) or not isinstance(raw_value, (int, str)):
        raise InvalidSymbolError(f"Binance COIN-M symbol {symbol} has invalid contractSize")
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise InvalidSymbolError(
            f"Binance COIN-M symbol {symbol} has invalid contractSize"
        ) from exc
    if value <= 0:
        raise InvalidSymbolError(f"Binance COIN-M symbol {symbol} has non-positive contractSize")
    return value


def _required_string(data: _Metadata, key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise InvalidSymbolError(f"Binance exchange-info row has invalid {key}")
    return value


def _required_filter(
    filters: dict[str, _Metadata],
    filter_type: str,
    symbol: str,
) -> _Metadata:
    filter_data = filters.get(filter_type)
    if filter_data is None:
        raise PlatformError(f"Binance {filter_type} filter is missing for {symbol}")
    return filter_data


def _positive_decimal(data: _Metadata, key: str, symbol: str) -> Decimal:
    value = _metadata_decimal(data, key, symbol)
    if value <= 0:
        raise PlatformError(f"Binance {key} must be positive for {symbol}")
    return value


def _non_negative_decimal(data: _Metadata, key: str, symbol: str) -> Decimal:
    value = _metadata_decimal(data, key, symbol)
    if value < 0:
        raise PlatformError(f"Binance {key} must not be negative for {symbol}")
    return value


def _metadata_decimal(data: _Metadata, key: str, symbol: str) -> Decimal:
    raw_value = data.get(key)
    if isinstance(raw_value, bool) or not isinstance(raw_value, (str, int, Decimal)):
        raise PlatformError(f"Binance {key} is missing or invalid for {symbol}")
    try:
        value = Decimal(str(raw_value))
    except (InvalidOperation, ValueError) as exc:
        raise PlatformError(f"Binance {key} is malformed for {symbol}") from exc
    if not value.is_finite():
        raise PlatformError(f"Binance {key} is not finite for {symbol}")
    return value


def _decimal_places(value: Decimal) -> int:
    value_tuple = value.as_tuple()
    exponent = value_tuple.exponent
    if not isinstance(exponent, int):
        return 0
    trailing_zeros = 0
    for digit in reversed(value_tuple.digits):
        if digit != 0:
            break
        trailing_zeros += 1
    return max(0, -(exponent + trailing_zeros))
