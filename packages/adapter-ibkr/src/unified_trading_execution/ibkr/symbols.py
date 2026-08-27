"""Instrument ↔ Interactive Brokers Contract translation.

IBKR identifies instruments with structured ``Contract`` objects (symbol,
secType, exchange, currency, expiry, strike, right, multiplier) rather than
plain symbol strings.  This module provides bi-directional, pure translation
helpers — no I/O, no qualification calls, no guessing.

Asset-class mapping (outbound):

    ==================  ========================  ==========================
    Unified             ib_async helper           IBKR ``secType``
    ==================  ========================  ==========================
    ``MARGIN_FX``       ``Forex``                 ``CASH``
    ``SPOT``            ``Crypto``                ``CRYPTO`` (Paxos/ZeroHash)
    ``STOCK``           ``Stock``                 ``STK``
    ``OPTION``          ``Option``                ``OPT``
    ``FUTURES``         ``Future``                ``FUT``
    ``CFD``             ``CFD``                   ``CFD``
    ==================  ========================  ==========================

Any other ``AssetClass`` raises ``InvalidSymbolError`` — the adapter never
approximates an unsupported instrument type.

Field conventions encoded here (ib_async type quirks):

- ``strike`` is a ``float`` on the wire (core carries ``Decimal``);
  converted via ``str()`` to avoid binary-float artefacts.
- ``multiplier`` is a **string** on the wire (core carries ``int``).
- Expiry is ``lastTradeDateOrContractMonth`` formatted ``YYYYMMDD``
  (or ``YYYYMM`` for a contract month); inbound ``YYYYMM`` maps to the
  first of that month.
- ``right`` is ``'C'``/``'P'`` (also accepts ``'CALL'``/``'PUT'`` inbound).

Fallbacks (per ``IBKRConfig``):

- ``exchange``: ``instrument.exchange`` else ``config.default_exchange``
  (empty everywhere leaves the ib_async default — ``IDEALPRO`` for Forex).
- ``currency``: non-pair instruments use ``instrument.currency`` else
  ``config.default_currency``; FX/crypto pairs use ``quote_currency``
  (required by core for pairs).

Venue-specific spelling: ``Instrument.platform_symbol`` maps to the IBKR
``localSymbol`` field (and back) when set.  It is excluded from core
identity (equality/hash), so round-trips stay consistent.

Identity caveat: ``exchange`` **is** part of core ``Instrument`` identity.
Inbound contracts carry their resolved exchange, so callers should construct
outbound instruments with the same exchange spelling they expect back
(or rely on the same ``default_exchange``) to keep state-store keys aligned.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from ib_async import CFD, Contract, Crypto, Forex, Future, Option, Stock

from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.types.enums import AssetClass, OptionRight
from unified_trading_execution.types.instrument import Instrument

if TYPE_CHECKING:
    from unified_trading_execution.ibkr.config import IBKRConfig

__all__ = ["from_ibkr_contract", "to_ibkr_contract"]


# ------------------------------------------------------------------
# Outbound: canonical Instrument -> ib_async Contract
# ------------------------------------------------------------------


def to_ibkr_contract(
    instrument: Instrument,
    config: IBKRConfig | None = None,
) -> Contract:
    """Convert a canonical ``Instrument`` to an ``ib_async.Contract``.

    See the module docstring for the asset-class map and field conventions.

    Raises
    ------
    InvalidSymbolError
        If the instrument's asset class is not supported by this adapter.
    ValueError
        If a required derivative field (expiry / strike / right /
        multiplier) is missing or malformed.  Core validates these at
        ``Instrument`` construction, so this is a defensive guard only.
    """
    exchange = _resolve_exchange(instrument, config)
    local = _local_symbol_kwarg(instrument)

    if instrument.asset_class is AssetClass.MARGIN_FX:
        return Forex(
            symbol=instrument.symbol,
            currency=_require_quote(instrument),
            **_exchange_kwarg(exchange),
            **local,
        )

    if instrument.asset_class is AssetClass.SPOT:
        return Crypto(
            symbol=instrument.symbol,
            currency=_require_quote(instrument),
            **_exchange_kwarg(exchange),
            **local,
        )

    if instrument.asset_class is AssetClass.STOCK:
        return Stock(
            instrument.symbol,
            **_exchange_kwarg(exchange),
            **_currency_kwargs(instrument, config),
            **local,
        )

    if instrument.asset_class is AssetClass.CFD:
        return CFD(
            instrument.symbol,
            **_exchange_kwarg(exchange),
            **_currency_kwargs(instrument, config),
            **local,
        )

    if instrument.asset_class is AssetClass.OPTION:
        return Option(
            instrument.symbol,
            lastTradeDateOrContractMonth=_format_expiry(instrument),
            strike=_strike_as_float(instrument),
            right=_option_right_wire(instrument),
            multiplier=_multiplier_as_str(instrument),
            **_exchange_kwarg(exchange),
            **_currency_kwargs(instrument, config),
            **local,
        )

    if instrument.asset_class is AssetClass.FUTURES:
        return Future(
            instrument.symbol,
            lastTradeDateOrContractMonth=_format_expiry(instrument),
            multiplier=_multiplier_as_str(instrument),
            **_exchange_kwarg(exchange),
            **_currency_kwargs(instrument, config),
            **local,
        )

    raise InvalidSymbolError(
        f"Asset class {instrument.asset_class} is not supported by the IBKR adapter "
        f"(supported: MARGIN_FX, SPOT, STOCK, OPTION, FUTURES, CFD)"
    )


# ------------------------------------------------------------------
# Inbound: ib_async Contract -> canonical Instrument
# ------------------------------------------------------------------


def from_ibkr_contract(contract: Contract) -> Instrument:
    """Convert an ``ib_async.Contract`` back to a canonical ``Instrument``.

    Parses ``secType``, ``symbol``, ``currency``, ``exchange``,
    ``lastTradeDateOrContractMonth``, ``strike``, ``right`` and
    ``multiplier`` into a fully populated ``Instrument``.  A non-empty
    ``localSymbol`` is preserved as ``platform_symbol`` (excluded from
    identity — see the module docstring).

    Raises
    ------
    ValueError
        If ``secType`` is unmapped, or a field required by the core
        ``Instrument`` model (expiry, strike, right, multiplier, currencies)
        is missing or malformed.
    """
    sec_type = (contract.secType or "").strip().upper()
    symbol = (contract.symbol or "").strip()
    if not symbol:
        raise ValueError("IBKR contract has an empty symbol")

    common: dict[str, Any] = {}
    if contract.exchange:
        common["exchange"] = contract.exchange
    if contract.localSymbol:
        common["platform_symbol"] = contract.localSymbol

    if sec_type == "CASH":
        return Instrument(
            symbol=symbol,
            quote_currency=_require_contract_currency(contract),
            asset_class=AssetClass.MARGIN_FX,
            **common,
        )

    if sec_type == "CRYPTO":
        return Instrument(
            symbol=symbol,
            quote_currency=_require_contract_currency(contract),
            asset_class=AssetClass.SPOT,
            **common,
        )

    if sec_type == "STK":
        return Instrument(
            symbol=symbol,
            asset_class=AssetClass.STOCK,
            currency=_optional_contract_currency(contract),
            **common,
        )

    if sec_type == "CFD":
        return Instrument(
            symbol=symbol,
            asset_class=AssetClass.CFD,
            currency=_optional_contract_currency(contract),
            **common,
        )

    if sec_type == "OPT":
        right = _parse_option_right(contract.right)
        return Instrument(
            symbol=symbol,
            asset_class=AssetClass.OPTION,
            currency=_optional_contract_currency(contract),
            expiry=_parse_expiry(contract.lastTradeDateOrContractMonth),
            strike=_parse_strike(contract.strike),
            option_right=right,
            multiplier=_parse_multiplier(contract.multiplier),
            **common,
        )

    if sec_type == "FUT":
        return Instrument(
            symbol=symbol,
            asset_class=AssetClass.FUTURES,
            currency=_require_contract_currency(contract),
            expiry=_parse_expiry(contract.lastTradeDateOrContractMonth),
            multiplier=_parse_multiplier(contract.multiplier),
            **common,
        )

    raise ValueError(f"Unmapped IBKR secType {contract.secType!r} for symbol {symbol!r}")


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _resolve_exchange(instrument: Instrument, config: IBKRConfig | None) -> str:
    """Instrument exchange, falling back to the configured default."""
    if instrument.exchange:
        return instrument.exchange
    return config.default_exchange if config is not None else ""


def _exchange_kwarg(exchange: str) -> dict[str, str]:
    """Pass ``exchange`` only when set, so ib_async defaults survive.

    ``Forex`` defaults its exchange to ``IDEALPRO``; forwarding an empty
    string would silently override that with "".
    """
    return {"exchange": exchange} if exchange else {}


def _local_symbol_kwarg(instrument: Instrument) -> dict[str, str]:
    """Forward ``platform_symbol`` as the IBKR ``localSymbol`` when set."""
    if instrument.platform_symbol:
        return {"localSymbol": instrument.platform_symbol}
    return {}


def _require_quote(instrument: Instrument) -> str:
    """Pair instruments must carry a quote currency (core enforces this)."""
    if not instrument.quote_currency:
        raise ValueError(
            f"Instrument {instrument.symbol!r} has no quote_currency — "
            f"{instrument.asset_class} is a BASE/QUOTE pair on IBKR"
        )
    return instrument.quote_currency


def _currency_kwargs(instrument: Instrument, config: IBKRConfig | None) -> dict[str, str]:
    """Non-pair contract currency: instrument value, then configured default."""
    if instrument.currency:
        return {"currency": instrument.currency}
    if config is not None:
        return {"currency": config.default_currency}
    return {}


def _format_expiry(instrument: Instrument) -> str:
    if instrument.expiry is None:
        raise ValueError(f"Instrument {instrument.symbol!r} has no expiry")
    return instrument.expiry.strftime("%Y%m%d")


def _strike_as_float(instrument: Instrument) -> float:
    if instrument.strike is None:
        raise ValueError(f"Instrument {instrument.symbol!r} has no strike")
    return float(instrument.strike)


def _multiplier_as_str(instrument: Instrument) -> str:
    if instrument.multiplier is None:
        raise ValueError(f"Instrument {instrument.symbol!r} has no multiplier")
    return str(instrument.multiplier)


def _option_right_wire(instrument: Instrument) -> str:
    if instrument.option_right is OptionRight.CALL:
        return "C"
    if instrument.option_right is OptionRight.PUT:
        return "P"
    raise ValueError(f"Instrument {instrument.symbol!r} has no option_right")


def _require_contract_currency(contract: Contract) -> str:
    currency = (contract.currency or "").strip()
    if not currency:
        raise ValueError(f"IBKR {contract.secType} contract {contract.symbol!r} has no currency")
    return currency


def _optional_contract_currency(contract: Contract) -> str | None:
    currency = (contract.currency or "").strip()
    return currency or None


def _parse_strike(raw: float) -> Decimal:
    if not raw or raw <= 0:
        raise ValueError(f"IBKR option contract has invalid strike {raw!r}")
    return Decimal(str(raw))


def _parse_multiplier(raw: str) -> int:
    text = (raw or "").strip()
    if not text:
        raise ValueError("IBKR derivative contract has no multiplier")
    try:
        return int(text)
    except ValueError:
        raise ValueError(f"IBKR multiplier {raw!r} is not an integer") from None


def _parse_expiry(raw: str) -> date:
    text = (raw or "").strip()
    try:
        if len(text) == 8:
            return datetime.strptime(text, "%Y%m%d").date()
        if len(text) == 6:
            return datetime.strptime(text, "%Y%m").date()
    except ValueError:
        pass
    raise ValueError(f"IBKR expiry {raw!r} is not YYYYMMDD or YYYYMM")


def _parse_option_right(raw: str) -> OptionRight:
    text = (raw or "").strip().upper()
    if text in ("C", "CALL"):
        return OptionRight.CALL
    if text in ("P", "PUT"):
        return OptionRight.PUT
    raise ValueError(f"IBKR option right {raw!r} is not Call/Put")
