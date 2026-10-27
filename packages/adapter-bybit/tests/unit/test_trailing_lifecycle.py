"""End-to-end trailing lifecycle through Engine + BybitAdapter.

A scripted venue stands in for pybit's HTTP session so the real adapter
translation, capability routing, state store, and engine wiring all run
together without network access.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.engine import Engine
from unified_trading_execution.events import EventBus
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.trailing import (
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
)
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument

_SYMBOL = "BTCUSDT"
_NOW_MS = str(int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000))


def _instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.FUTURES,
        exchange=None,
        currency="USDT",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=1,
    )


def _ok(result: dict[str, Any] | None = None) -> tuple[dict[str, Any], None, dict[str, str]]:
    return {"retCode": 0, "result": result or {}}, None, {}


def _listing() -> dict[str, Any]:
    return {
        "symbol": _SYMBOL,
        "baseCoin": "BTC",
        "quoteCoin": "USDT",
        "contractType": "LinearPerpetual",
        "status": "Trading",
        "lotSizeFilter": {
            "qtyStep": "0.001",
            "minOrderQty": "0.001",
            "maxOrderQty": "1000",
            "minNotionalValue": "5",
        },
        "priceFilter": {"tickSize": "0.10"},
    }


class ScriptedBybitVenue:
    """Minimal synchronous pybit-compatible venue for the trailing surface."""

    def __init__(self, *, last: Decimal = Decimal("50000")) -> None:
        self.last = last
        self.position: dict[str, Any] = {
            "symbol": _SYMBOL,
            "positionIdx": 0,
            "side": "Buy",
            "size": "1",
            "entryPrice": "50000",
            "updatedTime": _NOW_MS,
            "takeProfit": "",
            "stopLoss": "",
            "trailingStop": "",
            "activePrice": "",
        }
        self.trading_stop_calls: list[dict[str, Any]] = []

    def get_instruments_info(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        listings = [_listing()] if kwargs.get("category") == "linear" else []
        return _ok({"category": kwargs.get("category"), "list": listings})

    def get_account_info(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        return _ok({"marginMode": "REGULAR_MARGIN", "unifiedMarginStatus": 1})

    def set_margin_mode(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        return _ok()

    def get_wallet_balance(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        return _ok({"list": []})

    def get_positions(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        category = kwargs.get("category")
        settle_coin = kwargs.get("settleCoin")
        symbol = kwargs.get("symbol")
        if category != "linear" or settle_coin == "USDC":
            return _ok({"list": []})
        if symbol not in (None, _SYMBOL):
            return _ok({"list": []})
        return _ok({"list": [dict(self.position)]})

    def set_trading_stop(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        self.trading_stop_calls.append(dict(kwargs))
        trailing = kwargs.get("trailingStop")
        if trailing is not None:
            self.position["trailingStop"] = "" if str(trailing) == "0" else str(trailing)
        for field in ("takeProfit", "stopLoss"):
            if field in kwargs:
                value = str(kwargs[field])
                self.position[field] = "" if value == "0" else value
        active = kwargs.get("activePrice")
        if active is not None:
            self.position["activePrice"] = str(active)
        return _ok()

    def get_tickers(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        return _ok(
            {
                "list": [
                    {
                        "symbol": _SYMBOL,
                        "lastPrice": str(self.last),
                        "markPrice": str(self.last),
                        "bid1Price": str(self.last),
                        "ask1Price": str(self.last),
                    }
                ]
            }
        )


def _config() -> BybitConfig:
    return BybitConfig(
        api_key="test-key",
        api_secret="test-secret",
        testnet=True,
        account_id="engine-trailing-test",
    )


async def _engine(
    venue: ScriptedBybitVenue,
    store: SQLiteStateStore,
) -> tuple[Engine, BybitAdapter]:
    adapter = BybitAdapter(_config(), event_bus=EventBus())
    adapter._session = venue  # type: ignore[assignment]
    engine = Engine(
        adapter=adapter,
        state_store=store,
        event_bus=EventBus(),
        risk_config=RiskConfig(rate_limit_budget_override=100),
        reconcile_interval_seconds=None,
    )
    await engine.connect()
    return engine, adapter


async def _venue_stop(venue: ScriptedBybitVenue) -> Decimal | None:
    raw = venue.position["stopLoss"]
    return Decimal(raw) if raw else None


@pytest.mark.asyncio
async def test_native_attach_and_cancel_reach_the_venue(tmp_path: Path) -> None:
    venue = ScriptedBybitVenue()
    store = SQLiteStateStore(tmp_path / "native.db")
    engine, _ = await _engine(venue, store)
    try:
        state = await engine.trailing.attach(
            TrailingRequest(
                _instrument(),
                TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=Decimal("500")),
            )
        )

        assert state.mode is TrailingMode.NATIVE
        assert state.phase is TrailingPhase.ARMED
        assert venue.position["trailingStop"] == "500"
        assert await _venue_stop(venue) is None

        cancelled = await engine.trailing.cancel(state.trailing_id, clear_protection=True)

        assert cancelled.phase is TrailingPhase.CANCELLED
        assert venue.position["trailingStop"] == ""
    finally:
        await engine.ashutdown()


@pytest.mark.asyncio
async def test_synthetic_tightens_through_the_worker_and_clears(tmp_path: Path) -> None:
    venue = ScriptedBybitVenue()
    store = SQLiteStateStore(tmp_path / "synthetic.db")
    engine, _ = await _engine(venue, store)
    try:
        state = await engine.trailing.attach(
            TrailingRequest(
                _instrument(),
                TrailingSpec(
                    offset_type=TrailingOffsetType.PERCENT,
                    offset=Decimal("1"),
                ),
            )
        )

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.phase is TrailingPhase.ARMED
        assert state.current_stop == Decimal("49500.0")
        assert await _venue_stop(venue) == Decimal("49500.0")

        venue.last = Decimal("51000")
        worker = engine._trailing_worker
        assert worker is not None
        await worker.stop()
        await worker.run_once(now=time.monotonic() + 10.0)

        tightened = await engine.trailing.get(state.trailing_id)
        assert tightened is not None
        assert tightened.current_stop is not None
        assert tightened.current_stop > Decimal("49500.0")
        assert await _venue_stop(venue) == tightened.current_stop

        await engine.trailing.cancel(state.trailing_id, clear_protection=True)

        assert await _venue_stop(venue) is None
    finally:
        await engine.ashutdown()
