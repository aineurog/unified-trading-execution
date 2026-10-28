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
from pybit.exceptions import FailedRequestError

from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import PlatformConnectionError, TrailingStateError
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
from unified_trading_execution.types.position import Position

_SYMBOL = "BTCUSDT"
_INVERSE_SYMBOL = "BTCUSD"
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


def _inverse_instrument() -> Instrument:
    return Instrument(
        symbol="BTC",
        quote_currency="USD",
        asset_class=AssetClass.FUTURES,
        exchange=None,
        currency="BTC",
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=1,
    )


def _ok(result: dict[str, Any] | None = None) -> tuple[dict[str, Any], None, dict[str, str]]:
    return {"retCode": 0, "result": result or {}}, None, {}


def _listing(
    *,
    symbol: str = _SYMBOL,
    quote_coin: str = "USDT",
    contract_type: str = "LinearPerpetual",
    tick_size: str = "0.10",
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "baseCoin": "BTC",
        "quoteCoin": quote_coin,
        "contractType": contract_type,
        "status": "Trading",
        "lotSizeFilter": {
            "qtyStep": "0.001",
            "minOrderQty": "0.001",
            "maxOrderQty": "1000",
            "minNotionalValue": "5",
        },
        "priceFilter": {"tickSize": tick_size},
    }


def _leg(
    *,
    position_idx: int = 0,
    side: str = "Buy",
    size: str = "1",
    entry: str = "50000",
    symbol: str = _SYMBOL,
    category: str = "linear",
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "positionIdx": position_idx,
        "side": side,
        "size": size,
        "entryPrice": entry,
        "createdTime": _NOW_MS,
        "updatedTime": _NOW_MS,
        "takeProfit": "",
        "stopLoss": "",
        "trailingStop": "",
        "activePrice": "",
        "_category": category,
    }


class ScriptedBybitVenue:
    """Minimal synchronous pybit-compatible venue for the trailing surface."""

    def __init__(
        self,
        *,
        last: Decimal = Decimal("50000"),
        legs: list[dict[str, Any]] | None = None,
        listings: list[dict[str, Any]] | None = None,
    ) -> None:
        self.last = last
        self.positions = legs if legs is not None else [_leg()]
        self.position = self.positions[0]
        self.listings = listings if listings is not None else [_listing()]
        self.trading_stop_calls: list[dict[str, Any]] = []
        self.fail_trading_stop = False
        self.fail_tickers = False

    def _public_leg(self, leg: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in leg.items() if key != "_category"}

    def _find_leg(self, symbol: str, position_idx: int) -> dict[str, Any] | None:
        return next(
            (
                leg
                for leg in self.positions
                if leg["symbol"] == symbol and int(leg["positionIdx"]) == position_idx
            ),
            None,
        )

    def get_instruments_info(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        category = kwargs.get("category")
        symbol = kwargs.get("symbol")
        listings = [
            listing
            for listing in self.listings
            if listing["contractType"].startswith(category.title())
            and (symbol is None or listing["symbol"] == symbol)
        ]
        return _ok({"category": category, "list": listings})

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
        if category == "linear" and settle_coin == "USDC":
            return _ok({"list": []})
        legs = [
            self._public_leg(leg)
            for leg in self.positions
            if leg.get("_category") == category and (symbol is None or leg["symbol"] == symbol)
        ]
        return _ok({"list": legs})

    def set_trading_stop(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        self.trading_stop_calls.append(dict(kwargs))
        if self.fail_trading_stop:
            raise FailedRequestError(
                request="POST /v5/position/trading-stop",
                message="Internal Server Error",
                status_code=500,
                time="12:00:00",
                resp_headers=None,
            )
        leg = self._find_leg(str(kwargs["symbol"]), int(kwargs["positionIdx"]))
        if leg is None:
            return _ok()
        trailing = kwargs.get("trailingStop")
        if trailing is not None:
            leg["trailingStop"] = "" if str(trailing) == "0" else str(trailing)
        for field in ("takeProfit", "stopLoss"):
            if field in kwargs:
                value = str(kwargs[field])
                leg[field] = "" if value == "0" else value
        active = kwargs.get("activePrice")
        if active is not None:
            leg["activePrice"] = str(active)
        return _ok()

    def get_open_orders(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        return _ok({"list": []})

    def get_executions(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        return _ok({"list": []})

    def get_tickers(self, **kwargs: Any) -> tuple[dict[str, Any], None, dict[str, str]]:
        if self.fail_tickers:
            raise FailedRequestError(
                request="GET /v5/market/tickers",
                message="Internal Server Error",
                status_code=500,
                time="12:00:00",
                resp_headers=None,
            )
        symbol = kwargs.get("symbol") or _SYMBOL
        return _ok(
            {
                "list": [
                    {
                        "symbol": symbol,
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


async def _venue_stop(venue: ScriptedBybitVenue, position_idx: int = 0) -> Decimal | None:
    leg = venue._find_leg(_SYMBOL, position_idx)
    assert leg is not None
    raw = leg["stopLoss"]
    return Decimal(raw) if raw else None


async def _audit_events(store: SQLiteStateStore, event_type: str) -> list[dict[str, Any]]:
    return [
        event.payload
        for event in await store.query_audit_events(limit=100)
        if event.event_type == event_type
    ]


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


@pytest.mark.asyncio
async def test_synthetic_hedge_leg_isolated_from_partial_and_full_close(
    tmp_path: Path,
) -> None:
    venue = ScriptedBybitVenue(
        legs=[
            _leg(position_idx=1, side="Buy", size="1", entry="50000"),
            _leg(position_idx=2, side="Sell", size="1", entry="51000"),
        ]
    )
    store = SQLiteStateStore(tmp_path / "hedge.db")
    engine, _ = await _engine(venue, store)
    try:
        state = await engine.trailing.attach(
            TrailingRequest(
                _instrument(),
                TrailingSpec(
                    offset_type=TrailingOffsetType.PERCENT,
                    offset=Decimal("1"),
                ),
                position_id="BTCUSDT:1",
            )
        )

        assert state.mode is TrailingMode.SYNTHETIC
        assert state.position_id == "BTCUSDT:1"
        initial_stop = state.current_stop
        assert initial_stop == Decimal("49500.0")
        assert await _venue_stop(venue, 1) == initial_stop
        assert await _venue_stop(venue, 2) is None

        worker = engine._trailing_worker
        assert worker is not None
        await worker.stop()

        venue.last = Decimal("51000")
        base = time.monotonic()
        await worker.run_once(now=base + 10.0)
        tightened = await engine.trailing.get(state.trailing_id)
        assert tightened is not None
        assert tightened.current_stop is not None
        assert tightened.current_stop > initial_stop
        assert await _venue_stop(venue, 1) == tightened.current_stop
        assert await _venue_stop(venue, 2) is None

        venue.last = Decimal("49000")
        await worker.run_once(now=base + 20.0)
        pulled_back = await engine.trailing.get(state.trailing_id)
        assert pulled_back is not None
        assert pulled_back.current_stop == tightened.current_stop
        assert await _venue_stop(venue, 2) is None

        now = datetime.now(UTC)
        partial = Position(
            instrument=_instrument(),
            quantity=Decimal("0.5"),
            average_entry_price=Decimal("50000"),
            updated_at=now,
            position_id="BTCUSDT:1",
            opened_at=now,
        )
        resized = await engine.trailing.on_position_update(partial)
        assert resized is not None
        assert resized.quantity == Decimal("0.5")
        assert resized.current_stop == tightened.current_stop
        assert resized.phase is TrailingPhase.ARMED
        assert await _venue_stop(venue, 1) == tightened.current_stop

        closed = await engine.trailing.on_position_update(
            Position(
                instrument=_instrument(),
                quantity=Decimal("0"),
                average_entry_price=Decimal("50000"),
                updated_at=now,
                position_id="BTCUSDT:1",
                opened_at=now,
            )
        )
        assert closed is not None
        assert closed.phase is TrailingPhase.DONE
        assert closed.quantity == Decimal("0")
        assert await _venue_stop(venue, 1) is None
        assert await engine.trailing.list(active_only=True) == []
    finally:
        await engine.ashutdown()


@pytest.mark.asyncio
async def test_native_attach_refuses_then_takes_over_an_existing_trail(
    tmp_path: Path,
) -> None:
    venue = ScriptedBybitVenue()
    venue.position["trailingStop"] = "300"
    venue.position["takeProfit"] = "60000"
    store = SQLiteStateStore(tmp_path / "takeover.db")
    engine, _ = await _engine(venue, store)
    try:
        request = TrailingRequest(
            _instrument(),
            TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=Decimal("500")),
        )

        with pytest.raises(TrailingStateError, match="already has a native trailing stop"):
            await engine.trailing.attach(request)

        assert await engine.trailing.list(active_only=True) == []
        assert venue.position["trailingStop"] == "300"

        state = await engine.trailing.attach(
            TrailingRequest(
                _instrument(),
                TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=Decimal("500")),
                take_over_existing_protection=True,
            )
        )

        assert state.mode is TrailingMode.NATIVE
        assert state.phase is TrailingPhase.ARMED
        assert venue.position["trailingStop"] == "500"
        assert venue.position["takeProfit"] == "60000"

        takeover = await _audit_events(store, "trailing.takeover")
        assert takeover
        assert takeover[0]["previous_native_offset"] == "300"
        assert takeover[0]["current_stop"] is None

        now = datetime.now(UTC)
        partial = await engine.trailing.on_position_update(
            Position(
                instrument=_instrument(),
                quantity=Decimal("0.5"),
                average_entry_price=Decimal("50000"),
                updated_at=now,
                position_id="BTCUSDT:0",
                opened_at=now,
            )
        )
        assert partial is not None
        assert partial.quantity == Decimal("0.5")
        assert partial.phase is TrailingPhase.ARMED
        assert venue.position["trailingStop"] == "500"
    finally:
        await engine.ashutdown()


@pytest.mark.asyncio
async def test_native_state_rearms_after_restart_without_duplicates(
    tmp_path: Path,
) -> None:
    store = SQLiteStateStore(tmp_path / "native-restart.db")
    first_venue = ScriptedBybitVenue()
    first, _ = await _engine(first_venue, store)
    try:
        state = await first.trailing.attach(
            TrailingRequest(
                _instrument(),
                TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=Decimal("500")),
            )
        )
        assert state.phase is TrailingPhase.ARMED
        assert first_venue.position["trailingStop"] == "500"
    finally:
        await first.ashutdown()

    second_venue = ScriptedBybitVenue()
    second, _ = await _engine(second_venue, store)
    try:
        restored = await second.trailing.get(state.trailing_id)
        assert restored is not None
        assert restored.phase is TrailingPhase.ARMED
        assert restored.native_id == "BTCUSDT:0:trailing"
        assert second_venue.position["trailingStop"] == "500"
        assert len(await second.trailing.list(active_only=True)) == 1

        rearmed = await _audit_events(store, "trailing.integrity_restore")
        assert any(event["reason"] == "native_rearmed" for event in rearmed)

        with pytest.raises(TrailingStateError, match="already attached"):
            await second.trailing.attach(
                TrailingRequest(
                    _instrument(),
                    TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=Decimal("400")),
                )
            )
    finally:
        await second.ashutdown()


@pytest.mark.asyncio
async def test_auto_price_offset_routes_native_on_inverse(tmp_path: Path) -> None:
    venue = ScriptedBybitVenue(
        legs=[
            _leg(
                position_idx=0,
                side="Buy",
                size="100",
                entry="50000",
                symbol=_INVERSE_SYMBOL,
                category="inverse",
            )
        ],
        listings=[
            _listing(
                symbol=_INVERSE_SYMBOL,
                quote_coin="USD",
                contract_type="InversePerpetual",
                tick_size="0.5",
            )
        ],
    )
    store = SQLiteStateStore(tmp_path / "inverse.db")
    engine, _ = await _engine(venue, store)
    try:
        state = await engine.trailing.attach(
            TrailingRequest(
                _inverse_instrument(),
                TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=Decimal("500")),
            )
        )

        assert state.mode is TrailingMode.NATIVE
        assert state.phase is TrailingPhase.ARMED
        assert state.routing_reason == "native_exact"
        assert venue.position["trailingStop"] == "500"
        assert venue.trading_stop_calls[-1]["category"] == "inverse"
    finally:
        await engine.ashutdown()


@pytest.mark.asyncio
async def test_failed_native_place_leaves_no_active_state(tmp_path: Path) -> None:
    venue = ScriptedBybitVenue()
    venue.fail_trading_stop = True
    store = SQLiteStateStore(tmp_path / "native-failure.db")
    engine, _ = await _engine(venue, store)
    try:
        with pytest.raises(PlatformConnectionError):
            await engine.trailing.attach(
                TrailingRequest(
                    _instrument(),
                    TrailingSpec(offset_type=TrailingOffsetType.PRICE, offset=Decimal("500")),
                )
            )

        assert await engine.trailing.list(active_only=True) == []
        states = await engine.trailing.list()
        assert len(states) == 1
        assert states[0].phase is TrailingPhase.FAILED
        assert states[0].native_id is None
        assert states[0].last_error
        assert venue.position["trailingStop"] == ""
    finally:
        await engine.ashutdown()


@pytest.mark.asyncio
async def test_synthetic_ticker_failure_degrades_then_recovers(tmp_path: Path) -> None:
    venue = ScriptedBybitVenue()
    store = SQLiteStateStore(tmp_path / "degrade.db")
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
        worker = engine._trailing_worker
        assert worker is not None
        await worker.stop()

        for _ in range(5):
            await engine.trailing.on_ticker_failure(state.trailing_id, "ticker unavailable")

        degraded = await engine.trailing.get(state.trailing_id)
        assert degraded is not None
        assert degraded.phase is TrailingPhase.DEGRADED

        venue.last = Decimal("51000")
        await worker.run_once(now=time.monotonic() + 10.0)

        recovered = await engine.trailing.get(state.trailing_id)
        assert recovered is not None
        assert recovered.phase is TrailingPhase.ARMED
        assert recovered.current_stop is not None
        assert recovered.current_stop > state.current_stop  # type: ignore[operator]
    finally:
        await engine.ashutdown()
