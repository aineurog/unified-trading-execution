"""Engine-level Binance sandbox coverage for trailing stops and protection.

These tests drive ``engine.trailing`` (attach/cancel/get/list) against real
Demo/Testnet venues for USDⓈ-M and COIN-M Futures.  They complement the
adapter-level ``test_trailing.py`` by exercising core routing (AUTO/NATIVE/
SYNTHETIC), the persisted state store, restart rehydration, venue
``GTE_GTC`` auto-cancellation, and trigger behaviour end to end.

Every case opens a test-owned minimum-size position on a clean symbol,
asserts the routed mode and venue-side protection, and cleans up trailing
states, conditional/algo orders, regular orders, and the position itself.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from functools import partial
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from unified_trading_execution.binance import (
    BinanceAdapter,
    BinanceConfig,
    BinanceEngine,
    BinanceProduct,
)
from unified_trading_execution.binance.enums import PositionMode
from unified_trading_execution.binance.trailing import is_trailing_client_id
from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import (
    InvalidOrderError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    TrailingNotSupportedError,
    TrailingStateError,
)
from unified_trading_execution.events import EventBus
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.trailing import (
    ACTIVE_TRAILING_PHASES,
    CandleTimeframe,
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingRequest,
    TrailingSpec,
    TrailingState,
)
from unified_trading_execution.types.enums import (
    AssetClass,
    FillReason,
    OrderSide,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.order import (
    FillRecord,
    TpSlAttachment,
    UnifiedOrder,
)

from .test_trailing import (
    _COIN_M_POSITION_ID,
    _COIN_M_SYMBOL,
    _COIN_M_TEST_NOTIONAL_CAP,
    _USD_M_POSITION_ID,
    _USD_M_SYMBOL,
    _USD_M_TEST_NOTIONAL_CAP,
    _USD_M_TEST_NOTIONAL_TARGET,
    _coin_m_instrument,
    _usd_m_instrument,
    _wait_for_flat,
    _wait_for_leg_quantity,
)


def _ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step


def _floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


_PRODUCTS: dict[str, tuple[BinanceProduct, str, Instrument, str]] = {
    "usd_m": (
        BinanceProduct.USD_M,
        _USD_M_SYMBOL,
        _usd_m_instrument(),
        _USD_M_POSITION_ID,
    ),
    "coin_m": (
        BinanceProduct.COIN_M,
        _COIN_M_SYMBOL,
        _coin_m_instrument(),
        _COIN_M_POSITION_ID,
    ),
}

_PRODUCT_PARAMS = [pytest.param(key, key, id=key) for key in ("usd_m", "coin_m")]

# Long and short legs share the same one-way ``:BOTH`` position id; the sign of
# ``quantity`` carries the direction.  Side-parametrised cases exercise the
# mirrored stop/basis/activation logic for sells.
_PRODUCT_SIDE_PARAMS = [
    pytest.param("usd_m", "usd_m", OrderSide.BUY, id="usd_m-long"),
    pytest.param("usd_m", "usd_m", OrderSide.SELL, id="usd_m-short"),
    pytest.param("coin_m", "coin_m", OrderSide.BUY, id="coin_m-long"),
    pytest.param("coin_m", "coin_m", OrderSide.SELL, id="coin_m-short"),
]

_TERMINAL_PHASES = frozenset({TrailingPhase.DONE, TrailingPhase.CANCELLED, TrailingPhase.FAILED})

_CleanSymbol = Callable[[BinanceAdapter, BinanceProduct, str], Awaitable[None]]


def _spec(**overrides: object) -> TrailingSpec:
    values: dict[str, object] = {
        "offset_type": TrailingOffsetType.PERCENT,
        "offset": Decimal("0.1"),
    }
    values.update(overrides)
    return TrailingSpec(**values)  # type: ignore[arg-type]


def _request(
    instrument: Instrument,
    position_id: str,
    spec: TrailingSpec,
    *,
    mode: TrailingMode = TrailingMode.AUTO,
    take_over_existing_protection: bool = False,
) -> TrailingRequest:
    return TrailingRequest(
        instrument=instrument,
        spec=spec,
        position_id=position_id,
        mode=mode,
        take_over_existing_protection=take_over_existing_protection,
    )


async def _entry_quantity(
    adapter: BinanceAdapter,
    product: BinanceProduct,
    instrument: Instrument,
) -> Decimal:
    """Smallest valid test position, capped by the per-product notional budget."""
    spec = await adapter.fetch_instrument_spec(instrument)
    if product == BinanceProduct.COIN_M:
        contract_value = Decimal(str(instrument.multiplier))
        quantity = max(
            spec.min_qty,
            _ceil_to_step(
                max(spec.min_notional, contract_value) / contract_value,
                spec.lot_size,
            ),
        )
        if quantity * contract_value > _COIN_M_TEST_NOTIONAL_CAP:
            pytest.skip("Minimum COIN-M position exceeds the test notional cap")
        return quantity

    ticker = await adapter.fetch_ticker(instrument)
    assert ticker is not None and ticker.mark is not None
    quantity = max(
        spec.min_qty,
        _ceil_to_step(
            max(_USD_M_TEST_NOTIONAL_TARGET, spec.min_notional) / ticker.mark,
            spec.lot_size,
        ),
    )
    if quantity * ticker.mark > _USD_M_TEST_NOTIONAL_CAP:
        pytest.skip("Minimum USD-M position exceeds the test notional cap")
    return quantity


async def _cancel_symbol_orders(
    adapter: BinanceAdapter,
    product: BinanceProduct,
    symbol: str,
) -> None:
    """Best-effort cancel of every open regular and algo order on ``symbol``."""
    client = adapter._require_client()
    try:
        if product == BinanceProduct.USD_M:
            orders = await adapter._run_order_request(
                product,
                partial(client.futures_get_open_orders, symbol=symbol),
            )
            for row in orders if isinstance(orders, list) else []:
                with contextlib.suppress(Exception):
                    await adapter._run_order_request(
                        product,
                        partial(
                            client.futures_cancel_order,
                            symbol=symbol,
                            orderId=row.get("orderId"),
                        ),
                    )
            algo_orders = await adapter._run_order_request(
                product,
                partial(client.futures_get_open_algo_orders, symbol=symbol),
            )
            for row in algo_orders if isinstance(algo_orders, list) else []:
                with contextlib.suppress(Exception):
                    await adapter._run_order_request(
                        product,
                        partial(
                            client.futures_cancel_algo_order,
                            symbol=symbol,
                            algoId=row.get("algoId"),
                        ),
                    )
        else:
            orders = await adapter._run_order_request(
                product,
                partial(client.futures_coin_get_open_orders, symbol=symbol),
            )
            for row in orders if isinstance(orders, list) else []:
                with contextlib.suppress(Exception):
                    await adapter._run_order_request(
                        product,
                        partial(
                            client.futures_coin_cancel_order,
                            symbol=symbol,
                            orderId=row.get("orderId"),
                        ),
                    )
            algo_orders = await adapter._coin_m_algo_request(
                "get",
                "openAlgoOrders",
                {"symbol": symbol},
            )
            for row in algo_orders if isinstance(algo_orders, list) else []:
                with contextlib.suppress(Exception):
                    await adapter._coin_m_algo_request(
                        "delete",
                        "algoOrder",
                        {"symbol": symbol, "algoId": row.get("algoId")},
                    )
    except Exception:
        pass


async def _flatten_leg(
    adapter: BinanceAdapter,
    *,
    instrument: Instrument,
    product: BinanceProduct,
    symbol: str,
    position_id: str,
    entry_client_order_id: str,
    entry_filled_quantity: Decimal,
) -> None:
    """Cancel symbol orders, then close only the test-owned leg."""
    await _cancel_symbol_orders(adapter, product, symbol)
    if adapter.is_connected and entry_client_order_id in adapter._order_refs:
        with contextlib.suppress(OrderNotFoundError):
            entry_result = await adapter.get_order_by_client_id(entry_client_order_id)
            if entry_result is not None:
                entry_filled_quantity = max(entry_filled_quantity, entry_result.filled_quantity)
    for position in await adapter.fetch_positions():
        if position.position_id != position_id or position.quantity == 0:
            continue
        if abs(position.quantity) > entry_filled_quantity or entry_filled_quantity == 0:
            raise AssertionError(
                f"Refusing to close {symbol} exposure {abs(position.quantity)} "
                f"without confirming the test entry fill {entry_filled_quantity}"
            )
        await adapter.place_order(
            UnifiedOrder(
                instrument=instrument,
                order_type=OrderType.MARKET,
                side=OrderSide.SELL if position.quantity > 0 else OrderSide.BUY,
                quantity=abs(position.quantity),
                time_in_force=TimeInForce.GTC,
                reduce_only=True,
                client_order_id=f"engcl-{uuid4().hex[:16]}",
            )
        )
    await _wait_for_flat(adapter, position_id)


@dataclass
class _Harness:
    adapter: BinanceAdapter
    engine: Engine
    store: SQLiteStateStore
    instrument: Instrument
    product: BinanceProduct
    symbol: str
    position_id: str
    side: OrderSide
    quantity: Decimal
    entry_client_order_id: str
    entry_filled_quantity: Decimal


@asynccontextmanager
async def _open_harness(
    config: BinanceConfig,
    product_key: str,
    *,
    side: OrderSide = OrderSide.BUY,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> AsyncIterator[_Harness]:
    """Connect an adapter, open a minimum leg, then boot an Engine on it."""
    product, symbol, instrument, position_id = _PRODUCTS[product_key]
    adapter = BinanceAdapter(config, event_bus=event_bus)
    entry_client_order_id = f"eng-{product_key}-{uuid4().hex[:16]}"
    entry_filled_quantity = Decimal("0")
    engine: Engine | None = None
    store: SQLiteStateStore | None = None
    position_open = False
    try:
        await adapter.connect()
        await require_clean_futures_symbol(adapter, product, symbol)
        if await adapter.get_position_mode() is not PositionMode.ONE_WAY:
            pytest.skip("Trailing engine integration tests require one-way mode")

        quantity = await _entry_quantity(adapter, product, instrument)
        result = await adapter.place_order(
            UnifiedOrder(
                instrument=instrument,
                order_type=OrderType.MARKET,
                side=side,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
                client_order_id=entry_client_order_id,
            )
        )
        assert result.filled_quantity != 0
        entry_filled_quantity = result.filled_quantity
        leg_quantity = await _wait_for_leg_quantity(adapter, position_id)
        position_open = True

        store = SQLiteStateStore(str(tmp_path / f"{product_key}-trailing-engine.db"))
        engine = BinanceEngine(
            adapter,
            state_store=store,
            event_bus=event_bus,
            risk_config=RiskConfig(rate_limit_budget_override=1000),
            reconcile_interval_seconds=None,
        )
        await engine.connect()
        yield _Harness(
            adapter=adapter,
            engine=engine,
            store=store,
            instrument=instrument,
            product=product,
            symbol=symbol,
            position_id=position_id,
            side=side,
            quantity=leg_quantity,
            entry_client_order_id=entry_client_order_id,
            entry_filled_quantity=entry_filled_quantity,
        )
    finally:
        try:
            if engine is not None:
                with contextlib.suppress(Exception):
                    for state in await engine.trailing.list(active_only=True):
                        await engine.trailing.cancel(state.trailing_id, clear_protection=True)
            if not adapter.is_connected:
                # A test may have disconnected the adapter (degrade/recovery);
                # reconnect so venue-side cleanup can still run.
                with contextlib.suppress(Exception):
                    await adapter.connect()
            if adapter.is_connected:
                await _cancel_symbol_orders(adapter, product, symbol)
                if position_open:
                    with contextlib.suppress(Exception):
                        await _flatten_leg(
                            adapter,
                            instrument=instrument,
                            product=product,
                            symbol=symbol,
                            position_id=position_id,
                            entry_client_order_id=entry_client_order_id,
                            entry_filled_quantity=entry_filled_quantity,
                        )
        finally:
            if engine is not None:
                with contextlib.suppress(Exception):
                    await engine.ashutdown()
            elif adapter.is_connected:
                await adapter.disconnect()
            if store is not None:
                with contextlib.suppress(Exception):
                    await store.close()


async def _attach(
    engine: Engine,
    instrument: Instrument,
    position_id: str,
    spec: TrailingSpec,
    *,
    mode: TrailingMode = TrailingMode.AUTO,
    take_over_existing_protection: bool = False,
) -> TrailingState:
    return await engine.trailing.attach(
        _request(
            instrument,
            position_id,
            spec,
            mode=mode,
            take_over_existing_protection=take_over_existing_protection,
        )
    )


async def _cancel_trail(engine: Engine, state: TrailingState) -> None:
    await engine.trailing.cancel(state.trailing_id, clear_protection=True)


async def _wait_for_trail_done(
    harness: _Harness,
    trailing_id: str,
    *,
    timeout_seconds: float = 300.0,
) -> TrailingState:
    """Poll the persisted state until the trail reaches ``DONE``."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    last: TrailingState | None = None
    while asyncio.get_running_loop().time() < deadline:
        state = await harness.engine.trailing.get(trailing_id)
        if state is not None:
            last = state
            if state.phase is TrailingPhase.DONE:
                return state
        await asyncio.sleep(5)
    raise AssertionError(
        f"Trailing state {trailing_id} did not reach DONE within "
        f"{timeout_seconds}s (last phase={last.phase if last else None}, "
        f"error={last.last_error if last else None})"
    )


async def _assert_leg_flat(adapter: BinanceAdapter, position_id: str) -> None:
    positions = await adapter.fetch_positions()
    assert all(p.position_id != position_id or p.quantity == 0 for p in positions), (
        f"position {position_id} still open"
    )


# ---------------------------------------------------------------------------
# AUTO routing matrices
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("product_key", "binance_order_config", "side"),
    _PRODUCT_SIDE_PARAMS,
    indirect=["binance_order_config"],
)
async def test_auto_native_param_matrix(
    product_key: str,
    side: OrderSide,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """Every native-expressible spec must route NATIVE under AUTO on Futures."""
    async with _open_harness(
        binance_order_config,
        product_key,
        side=side,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]
        ticker = await harness.adapter.fetch_ticker(instrument)
        assert ticker is not None and ticker.mark is not None
        spec_info = await harness.adapter.fetch_instrument_spec(instrument)
        # Activation must sit on the unreached side of the mark: a long trail
        # activates when price rises to the target, a short trail when it
        # falls.  Targets already satisfied make the venue reject the order.
        if side is OrderSide.BUY:
            activation_price = _ceil_to_step(ticker.mark * Decimal("1.05"), spec_info.tick_size)
        else:
            activation_price = _floor_to_step(ticker.mark * Decimal("0.95"), spec_info.tick_size)

        cases = [
            _spec(offset=Decimal("0.1"), basis=TrailingBasis.LAST),
            _spec(offset=Decimal("0.5"), basis=TrailingBasis.MARK),
            _spec(offset=Decimal("1"), basis=None),
            _spec(
                offset=Decimal("1.1"),
                basis=TrailingBasis.LAST,
                activation=TrailingActivation.PRICE,
                activation_price=activation_price,
            ),
            _spec(
                offset=Decimal("1.0"),
                basis=TrailingBasis.LAST,
                activation=TrailingActivation.PROFIT_PERCENT,
                activation_profit_percent=Decimal("2"),
            ),
            _spec(
                offset=Decimal("1.0"),
                basis=TrailingBasis.LAST,
                take_profit=ProtectionLevel(ProtectionLevelType.PERCENT, Decimal("10")),
            ),
        ]
        for spec in cases:
            try:
                state = await _attach(harness.engine, instrument, position_id, spec)
            except (PlatformError, InvalidOrderError) as exc:
                pytest.fail(f"attach failed for {spec}: {exc}")
            assert state.mode is TrailingMode.NATIVE, (
                f"{spec} routed to {state.mode} ({state.routing_reason})"
            )
            assert state.native_id is not None
            if state.native_id is not None:
                assert is_trailing_client_id(state.native_id)
            venue = await harness.adapter.get_position_trailing_stop(instrument, position_id)
            assert venue is not None, f"venue has no trailing order for {spec}"
            await _cancel_trail(harness.engine, state)
            venue = await harness.adapter.get_position_trailing_stop(instrument, position_id)
            assert venue is None


@pytest.mark.parametrize(
    ("product_key", "binance_order_config", "side"),
    _PRODUCT_SIDE_PARAMS,
    indirect=["binance_order_config"],
)
async def test_auto_synthetic_param_matrix(
    product_key: str,
    side: OrderSide,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """Specs native cannot express must fall back to SYNTHETIC under AUTO."""
    async with _open_harness(
        binance_order_config,
        product_key,
        side=side,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]
        spec_info = await harness.adapter.fetch_instrument_spec(instrument)
        ticker = await harness.adapter.fetch_ticker(instrument)
        assert ticker is not None and ticker.last is not None
        price_distance = (ticker.last * Decimal("0.01")).quantize(Decimal("0.1"))
        # A static stop sits on the losing side: below entry for longs, above
        # for shorts.
        raw_stop = ticker.last * (Decimal("0.9") if side is OrderSide.BUY else Decimal("1.1"))
        initial_stop_price = (
            _floor_to_step(raw_stop, spec_info.tick_size)
            if side is OrderSide.BUY
            else _ceil_to_step(raw_stop, spec_info.tick_size)
        )

        cases = [
            _spec(offset=Decimal("1.0"), basis=TrailingBasis.BID),
            _spec(offset=Decimal("1.0"), basis=TrailingBasis.ASK),
            _spec(offset_type=TrailingOffsetType.PRICE, offset=price_distance),
            _spec(
                offset_type=TrailingOffsetType.ATR,
                offset=Decimal("1.5"),
                atr_period=14,
                atr_timeframe=CandleTimeframe.M15,
            ),
            _spec(
                offset=Decimal("1.0"),
                basis=TrailingBasis.LAST,
                initial_stop=ProtectionLevel(ProtectionLevelType.PERCENT, Decimal("8")),
            ),
            _spec(
                offset=Decimal("1.0"),
                basis=TrailingBasis.LAST,
                initial_stop=ProtectionLevel(ProtectionLevelType.PRICE, initial_stop_price),
            ),
            _spec(
                offset=Decimal("1.0"),
                basis=TrailingBasis.LAST,
                initial_stop=ProtectionLevel(ProtectionLevelType.ATR, Decimal("1.5")),
                atr_period=14,
                atr_timeframe=CandleTimeframe.M15,
            ),
            _spec(offset=Decimal("1.0"), basis=TrailingBasis.LAST, step=Decimal("50")),
        ]
        for spec in cases:
            state = await _attach(harness.engine, instrument, position_id, spec)
            assert state.mode is TrailingMode.SYNTHETIC, (
                f"{spec} routed to {state.mode} ({state.routing_reason})"
            )
            assert state.native_id is None
            assert state.current_stop is not None, f"no stop written for {spec}"
            await _cancel_trail(harness.engine, state)
            confirmed = await harness.adapter.get_position_tpsl(instrument, position_id)
            assert confirmed in (None, (None, None)), (
                f"protection orders survived cancel for {spec}: {confirmed}"
            )


# ---------------------------------------------------------------------------
# Explicit modes, capability boundaries, and unsupported requests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("product_key", "binance_order_config"), _PRODUCT_PARAMS, indirect=["binance_order_config"]
)
async def test_explicit_modes_and_unsupported_requests(
    product_key: str,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """Forced modes honour capability boundaries; unsamplable bases are rejected."""
    async with _open_harness(
        binance_order_config,
        product_key,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]

        # Forced NATIVE accepts an exact native request.
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            _spec(offset=Decimal("1"), basis=TrailingBasis.LAST),
            mode=TrailingMode.NATIVE,
        )
        assert state.mode is TrailingMode.NATIVE
        await _cancel_trail(harness.engine, state)

        # Forced SYNTHETIC accepts even a native-expressible request.
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            _spec(offset=Decimal("1"), basis=TrailingBasis.LAST),
            mode=TrailingMode.SYNTHETIC,
        )
        assert state.mode is TrailingMode.SYNTHETIC
        await _cancel_trail(harness.engine, state)

        # Forced NATIVE rejects every dimension outside the venue capability.
        native_rejections = [
            _spec(offset_type=TrailingOffsetType.PRICE, offset=Decimal("50")),
            _spec(
                offset_type=TrailingOffsetType.ATR,
                offset=Decimal("1.5"),
                atr_period=14,
                atr_timeframe=CandleTimeframe.M15,
            ),
            _spec(offset=Decimal("1"), basis=TrailingBasis.BID),
            _spec(offset=Decimal("1"), basis=TrailingBasis.ASK),
            _spec(
                offset=Decimal("1"),
                initial_stop=ProtectionLevel(ProtectionLevelType.PERCENT, Decimal("8")),
            ),
        ]
        for spec in native_rejections:
            with pytest.raises(TrailingNotSupportedError):
                await _attach(
                    harness.engine,
                    instrument,
                    position_id,
                    spec,
                    mode=TrailingMode.NATIVE,
                )

        # Take-profit is not part of the venue trail; core writes it through
        # position protection alongside a native trail.
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            _spec(
                offset=Decimal("1"),
                basis=TrailingBasis.LAST,
                take_profit=ProtectionLevel(ProtectionLevelType.PERCENT, Decimal("10")),
            ),
            mode=TrailingMode.NATIVE,
        )
        assert state.mode is TrailingMode.NATIVE
        assert state.take_profit_owned is True
        await _cancel_trail(harness.engine, state)
        confirmed = await harness.adapter.get_position_tpsl(instrument, position_id)
        assert confirmed in (None, (None, None))

        # MID is not a samplable Binance basis in any mode.
        with pytest.raises(TrailingNotSupportedError):
            await _attach(
                harness.engine,
                instrument,
                position_id,
                _spec(offset=Decimal("1"), basis=TrailingBasis.MID),
            )

        # PROFIT_PERCENT activation maps to a price activation and stays native.
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            _spec(
                offset=Decimal("1"),
                basis=TrailingBasis.LAST,
                activation=TrailingActivation.PROFIT_PERCENT,
                activation_profit_percent=Decimal("1.5"),
            ),
        )
        assert state.mode is TrailingMode.NATIVE
        await _cancel_trail(harness.engine, state)

        # Active-state uniqueness: a second attach on the same leg is refused.
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            _spec(offset=Decimal("1"), basis=TrailingBasis.LAST),
            mode=TrailingMode.SYNTHETIC,
        )
        with pytest.raises(TrailingStateError, match="already attached"):
            await _attach(
                harness.engine,
                instrument,
                position_id,
                _spec(offset=Decimal("2"), basis=TrailingBasis.LAST),
                mode=TrailingMode.SYNTHETIC,
            )
        await _cancel_trail(harness.engine, state)


@pytest.mark.parametrize("binance_order_config", ["spot"], indirect=True)
async def test_spot_trailing_attach_rejected(
    binance_order_config: BinanceConfig,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """Spot has no position legs or synthetic protection; attach must fail fast."""
    instrument = Instrument(
        symbol="BTC",
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        platform_symbol="BTCUSDT",
    )
    store = SQLiteStateStore(str(tmp_path / "spot-trailing.db"))
    engine = BinanceEngine(
        binance_order_config,
        state_store=store,
        event_bus=event_bus,
        risk_config=RiskConfig(rate_limit_budget_override=1000),
        reconcile_interval_seconds=None,
    )
    try:
        await engine.connect()
        with pytest.raises((InvalidOrderError, TrailingNotSupportedError)):
            await engine.trailing.attach(
                TrailingRequest(
                    instrument=instrument,
                    spec=_spec(offset=Decimal("1")),
                )
            )
    finally:
        with contextlib.suppress(Exception):
            await engine.ashutdown()


# ---------------------------------------------------------------------------
# Trigger behaviour and venue lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("product_key", "binance_order_config", "side"),
    [
        pytest.param("usd_m", "usd_m", OrderSide.BUY, id="usd_m-long"),
        pytest.param("usd_m", "usd_m", OrderSide.SELL, id="usd_m-short"),
    ],
    indirect=["binance_order_config"],
)
async def test_synthetic_trail_triggers_and_closes_position(
    product_key: str,
    side: OrderSide,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """A tight synthetic trail must fire and flatten the leg end to end."""
    async with _open_harness(
        binance_order_config,
        product_key,
        side=side,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]
        spec_info = await harness.adapter.fetch_instrument_spec(instrument)
        ticker = await harness.adapter.fetch_ticker(instrument)
        assert ticker is not None and ticker.last is not None
        # Price offset of ~0.05%: tight enough that ordinary noise triggers
        # it soon, wide enough to survive the attach/confirm window.
        offset = max(
            spec_info.tick_size * 10,
            _ceil_to_step(ticker.last * Decimal("0.0005"), spec_info.tick_size),
        )
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            _spec(offset_type=TrailingOffsetType.PRICE, offset=offset),
            mode=TrailingMode.SYNTHETIC,
        )
        assert state.mode is TrailingMode.SYNTHETIC
        done = await _wait_for_trail_done(harness, state.trailing_id)
        assert done.phase is TrailingPhase.DONE
        await _assert_leg_flat(harness.adapter, position_id)

        # The account stream must surface the exit as TRAILING_STOP, not a
        # plain stop loss — synthetic exits are reclassified by core.
        deadline = asyncio.get_running_loop().time() + 30
        fills: list[FillRecord] = []
        while asyncio.get_running_loop().time() < deadline:
            fills = await harness.store.query_fills(instrument=instrument)
            if any(f.reason is FillReason.TRAILING_STOP for f in fills):
                break
            await asyncio.sleep(2)
        assert any(f.reason is FillReason.TRAILING_STOP for f in fills), (
            f"no TRAILING_STOP fill recorded; fills={[(f.reason, f.fill_price) for f in fills]}"
        )


@pytest.mark.parametrize(
    ("product_key", "binance_order_config"), _PRODUCT_PARAMS, indirect=["binance_order_config"]
)
async def test_native_gte_gtc_autocancel_on_manual_close(
    product_key: str,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """GTE_GTC native trails must vanish when the position is closed manually."""
    async with _open_harness(
        binance_order_config,
        product_key,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            _spec(offset=Decimal("5"), basis=TrailingBasis.LAST),
        )
        assert state.mode is TrailingMode.NATIVE
        venue = await harness.adapter.get_position_trailing_stop(instrument, position_id)
        assert venue is not None

        # Manual close: the venue must auto-cancel the position-linked trail.
        for position in await harness.adapter.fetch_positions():
            if position.position_id == position_id and position.quantity != 0:
                await harness.adapter.place_order(
                    UnifiedOrder(
                        instrument=instrument,
                        order_type=OrderType.MARKET,
                        side=(OrderSide.SELL if position.quantity > 0 else OrderSide.BUY),
                        quantity=abs(position.quantity),
                        time_in_force=TimeInForce.GTC,
                        reduce_only=True,
                        client_order_id=f"mancl-{uuid4().hex[:16]}",
                    )
                )
        await _wait_for_flat(harness.adapter, position_id)

        deadline = asyncio.get_running_loop().time() + 30
        venue = await harness.adapter.get_position_trailing_stop(instrument, position_id)
        while venue is not None and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(1)
            venue = await harness.adapter.get_position_trailing_stop(instrument, position_id)
        assert venue is None, "GTE_GTC trail survived a manual position close"

        done = await _wait_for_trail_done(harness, state.trailing_id, timeout_seconds=60)
        assert done.phase is TrailingPhase.DONE


@pytest.mark.parametrize(
    ("product_key", "binance_order_config"), _PRODUCT_PARAMS, indirect=["binance_order_config"]
)
async def test_static_tpsl_then_synthetic_trail_preserves_unowned_sides(
    product_key: str,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """Adapter-level static TP/SL must survive a synthetic trail attach/cancel."""
    async with _open_harness(
        binance_order_config,
        product_key,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]
        spec = await harness.adapter.fetch_instrument_spec(instrument)
        ticker = await harness.adapter.fetch_ticker(instrument)
        assert ticker is not None and ticker.mark is not None
        stop_trigger = _floor_to_step(ticker.mark * Decimal("0.5"), spec.tick_size)
        tp_trigger = _ceil_to_step(ticker.mark * Decimal("1.5"), spec.tick_size)
        await harness.adapter.modify_position_tpsl(
            instrument,
            position_id,
            stop_loss=TpSlAttachment(trigger_price=stop_trigger),
            take_profit=TpSlAttachment(trigger_price=tp_trigger),
            trigger_basis=TrailingBasis.MARK,
        )
        confirmed = await harness.adapter.get_position_tpsl(instrument, position_id)
        assert confirmed is not None and confirmed != (None, None)

        state: TrailingState | None = None
        try:
            try:
                state = await _attach(
                    harness.engine,
                    instrument,
                    position_id,
                    _spec(offset=Decimal("1"), basis=TrailingBasis.LAST),
                    mode=TrailingMode.SYNTHETIC,
                )
            except TrailingStateError:
                state = await _attach(
                    harness.engine,
                    instrument,
                    position_id,
                    _spec(offset=Decimal("1"), basis=TrailingBasis.LAST),
                    mode=TrailingMode.SYNTHETIC,
                    take_over_existing_protection=True,
                )
            assert state.mode is TrailingMode.SYNTHETIC
            confirmed = await harness.adapter.get_position_tpsl(instrument, position_id)
            assert confirmed is not None and confirmed != (None, None), (
                "static TP/SL disappeared when the synthetic trail attached"
            )
            # Cancel without clear_protection: the trail must not remove
            # protection it does not own.
            await harness.engine.trailing.cancel(state.trailing_id)
            state = None
            confirmed = await harness.adapter.get_position_tpsl(instrument, position_id)
            assert confirmed is not None and confirmed != (None, None), (
                "unowned static TP/SL was removed by trail cancellation"
            )
        finally:
            await harness.adapter.clear_position_tpsl(
                instrument, position_id, take_profit=True, stop_loss=True
            )
            confirmed = await harness.adapter.get_position_tpsl(instrument, position_id)
            assert confirmed in (None, (None, None))


@pytest.mark.parametrize(
    ("product_key", "binance_order_config"), _PRODUCT_PARAMS, indirect=["binance_order_config"]
)
async def test_restart_rehydrates_native_trail(
    product_key: str,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """A native trail must survive an engine restart via the state store."""
    product, symbol, instrument, position_id = _PRODUCTS[product_key]
    store_path = tmp_path / f"{product_key}-restart.db"
    store: SQLiteStateStore | None = SQLiteStateStore(str(store_path))
    adapter = BinanceAdapter(binance_order_config, event_bus=event_bus)
    entry_client_order_id = f"rst-{product_key}-{uuid4().hex[:16]}"
    engine: Engine | None = None
    engine2: Engine | None = None
    adapter2: BinanceAdapter | None = None
    store2: SQLiteStateStore | None = None
    position_open = False
    entry_filled = Decimal("0")
    trailing_id: str | None = None
    native_id: str | None = None
    try:
        await adapter.connect()
        await require_clean_futures_symbol(adapter, product, symbol)
        if await adapter.get_position_mode() is not PositionMode.ONE_WAY:
            pytest.skip("Trailing engine integration tests require one-way mode")
        quantity = await _entry_quantity(adapter, product, instrument)
        result = await adapter.place_order(
            UnifiedOrder(
                instrument=instrument,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
                client_order_id=entry_client_order_id,
            )
        )
        entry_filled = result.filled_quantity
        await _wait_for_leg_quantity(adapter, position_id)
        position_open = True

        engine = BinanceEngine(
            adapter,
            state_store=store,
            event_bus=event_bus,
            risk_config=RiskConfig(rate_limit_budget_override=1000),
            reconcile_interval_seconds=None,
        )
        await engine.connect()
        state = await _attach(
            engine,
            instrument,
            position_id,
            _spec(offset=Decimal("2"), basis=TrailingBasis.LAST),
        )
        assert state.mode is TrailingMode.NATIVE
        trailing_id = state.trailing_id
        native_id = state.native_id
        assert native_id is not None
        await engine.ashutdown()
        engine = None
        store = None  # closed by ashutdown

        # Reboot on the same store path: the trail must rehydrate, not duplicate.
        store2 = SQLiteStateStore(str(store_path))
        adapter2 = BinanceAdapter(binance_order_config, event_bus=event_bus)
        engine2 = BinanceEngine(
            adapter2,
            state_store=store2,
            event_bus=event_bus,
            risk_config=RiskConfig(rate_limit_budget_override=1000),
            reconcile_interval_seconds=None,
        )
        await engine2.connect()
        rehydrated = await engine2.trailing.get(trailing_id)
        assert rehydrated is not None, "trailing state lost across restart"
        assert rehydrated.mode is TrailingMode.NATIVE
        assert rehydrated.native_id == native_id
        assert rehydrated.phase in ACTIVE_TRAILING_PHASES
        venue = await adapter2.get_position_trailing_stop(instrument, position_id)
        assert venue is not None and venue.native_id == native_id

        await engine2.trailing.cancel(trailing_id, clear_protection=True)
        venue = await adapter2.get_position_trailing_stop(instrument, position_id)
        assert venue is None
        trailing_id = None
    finally:
        try:
            for eng in (engine2, engine):
                if eng is not None:
                    with contextlib.suppress(Exception):
                        for state in await eng.trailing.list(active_only=True):
                            await eng.trailing.cancel(state.trailing_id, clear_protection=True)
            for adp in (adapter2, adapter):
                if adp is not None and adp.is_connected:
                    await _cancel_symbol_orders(adp, product, symbol)
            if position_open:
                for adp in (adapter2, adapter):
                    if adp is not None and adp.is_connected:
                        with contextlib.suppress(Exception):
                            await _flatten_leg(
                                adp,
                                instrument=instrument,
                                product=product,
                                symbol=symbol,
                                position_id=position_id,
                                entry_client_order_id=entry_client_order_id,
                                entry_filled_quantity=entry_filled,
                            )
                        break
        finally:
            for eng in (engine2, engine):
                if eng is not None:
                    with contextlib.suppress(Exception):
                        await eng.ashutdown()
            for adp in (adapter2, adapter):
                if adp is not None and adp.is_connected:
                    with contextlib.suppress(Exception):
                        await adp.disconnect()
            for st in (store2, store):
                if st is not None:
                    with contextlib.suppress(Exception):
                        await st.close()


# ---------------------------------------------------------------------------
# Activation gating, repricing, resilience, and position-mode boundaries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("product_key", "binance_order_config", "side"),
    _PRODUCT_SIDE_PARAMS,
    indirect=["binance_order_config"],
)
async def test_synthetic_activation_price_and_profit_percent(
    product_key: str,
    side: OrderSide,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """An unreached activation must leave the leg unprotected until reached.

    For a long leg activation is reached when the basis rises to the target;
    for a short it is reached when the basis falls to it.  Both cases below
    pick targets on the unreached side of the current mark.
    """
    async with _open_harness(
        binance_order_config,
        product_key,
        side=side,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]
        spec_info = await harness.adapter.fetch_instrument_spec(instrument)
        ticker = await harness.adapter.fetch_ticker(instrument)
        assert ticker is not None and ticker.mark is not None
        mark = ticker.mark

        if side is OrderSide.BUY:
            raw_activation = mark * Decimal("1.05")
            activation_price = _ceil_to_step(raw_activation, spec_info.tick_size)
        else:
            raw_activation = mark * Decimal("0.95")
            activation_price = _floor_to_step(raw_activation, spec_info.tick_size)

        cases = [
            _spec(
                offset=Decimal("1.0"),
                basis=TrailingBasis.LAST,
                activation=TrailingActivation.PRICE,
                activation_price=activation_price,
            ),
            _spec(
                offset=Decimal("1.0"),
                basis=TrailingBasis.LAST,
                activation=TrailingActivation.PROFIT_PERCENT,
                activation_profit_percent=Decimal("2"),
            ),
        ]
        for spec in cases:
            state = await _attach(
                harness.engine, instrument, position_id, spec, mode=TrailingMode.SYNTHETIC
            )
            assert state.mode is TrailingMode.SYNTHETIC
            assert state.activated is False, f"activation reached early for {spec}"
            assert state.current_stop is None, f"stop written before activation for {spec}"
            confirmed = await harness.adapter.get_position_tpsl(instrument, position_id)
            assert confirmed in (None, (None, None)), (
                f"venue protection exists before activation for {spec}: {confirmed}"
            )
            await _cancel_trail(harness.engine, state)


@pytest.mark.parametrize(
    ("product_key", "binance_order_config", "side"),
    _PRODUCT_SIDE_PARAMS,
    indirect=["binance_order_config"],
)
async def test_synthetic_trail_reprices_on_favorable_move(
    product_key: str,
    side: OrderSide,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
) -> None:
    """A wide synthetic trail must ratchet its stop on favourable ticks."""
    async with _open_harness(
        binance_order_config,
        product_key,
        side=side,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            # 10% offset: far enough that ordinary noise cannot trigger it.
            _spec(offset=Decimal("10"), basis=TrailingBasis.LAST),
            mode=TrailingMode.SYNTHETIC,
        )
        assert state.mode is TrailingMode.SYNTHETIC

        deadline = asyncio.get_running_loop().time() + 180
        last: TrailingState | None = None
        while asyncio.get_running_loop().time() < deadline:
            last = await harness.engine.trailing.get(state.trailing_id)
            if last is not None and last.reprice_count > 0:
                break
            await asyncio.sleep(3)
        assert last is not None and last.reprice_count > 0, f"trail never repriced; last={last}"
        await _cancel_trail(harness.engine, last)


@pytest.mark.parametrize(
    ("product_key", "binance_order_config"), _PRODUCT_PARAMS, indirect=["binance_order_config"]
)
async def test_synthetic_trail_degrades_and_recovers_on_ticker_outage(
    product_key: str,
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ticker fetch failures must degrade the trail; recovery must re-arm it.

    The outage is simulated by failing ``fetch_ticker`` rather than
    ``adapter.disconnect()``: disconnect flips ``is_connected`` and the
    trailing API correctly refuses all calls while the engine is down.
    """
    async with _open_harness(
        binance_order_config,
        product_key,
        require_clean_futures_symbol=require_clean_futures_symbol,
        event_bus=event_bus,
        tmp_path=tmp_path,
    ) as harness:
        _, _, instrument, position_id = _PRODUCTS[product_key]
        state = await _attach(
            harness.engine,
            instrument,
            position_id,
            _spec(offset=Decimal("5"), basis=TrailingBasis.LAST),
            mode=TrailingMode.SYNTHETIC,
        )
        assert state.mode is TrailingMode.SYNTHETIC

        original_fetch_ticker = harness.adapter.fetch_ticker

        async def failing_fetch_ticker(*args: Any, **kwargs: Any) -> Any:
            raise PlatformConnectionError("simulated ticker outage")

        monkeypatch.setattr(harness.adapter, "fetch_ticker", failing_fetch_ticker)
        try:
            deadline = asyncio.get_running_loop().time() + 90
            degraded: TrailingState | None = None
            while asyncio.get_running_loop().time() < deadline:
                current = await harness.engine.trailing.get(state.trailing_id)
                if current is not None and current.phase is TrailingPhase.DEGRADED:
                    degraded = current
                    break
                await asyncio.sleep(3)
            assert degraded is not None, "trail never degraded after ticker outage"
        finally:
            monkeypatch.setattr(harness.adapter, "fetch_ticker", original_fetch_ticker)

        deadline = asyncio.get_running_loop().time() + 90
        recovered: TrailingState | None = None
        latest: TrailingState | None = None
        while asyncio.get_running_loop().time() < deadline:
            latest = await harness.engine.trailing.get(state.trailing_id)
            if latest is not None and latest.phase is TrailingPhase.ARMED:
                recovered = latest
                break
            await asyncio.sleep(3)
        assert recovered is not None, f"trail did not re-arm after recovery; last={latest}"
        await _cancel_trail(harness.engine, recovered)


@pytest.mark.parametrize("binance_order_config", ["usd_m"], indirect=True)
async def test_hedge_mode_order_entry_rejected(
    binance_order_config: BinanceConfig,
    require_clean_futures_symbol: _CleanSymbol,
    event_bus: EventBus,
) -> None:
    """Hedge mode is account-scoped; regular order payloads cannot scope
    ``positionSide``, so entry orders must fail rather than misroute."""
    adapter = BinanceAdapter(binance_order_config, event_bus=event_bus)
    await adapter.connect()
    previous_mode: PositionMode | None = None
    try:
        previous_mode = await adapter.get_position_mode()
        if previous_mode is PositionMode.HEDGE:
            pytest.skip("Account is already in hedge mode; nothing to verify")
        try:
            await adapter.set_position_mode(PositionMode.HEDGE)
        except PlatformError as exc:
            pytest.skip(f"Cannot enter hedge mode on this account: {exc}")

        instrument = _usd_m_instrument()
        spec = await adapter.fetch_instrument_spec(instrument)
        order = UnifiedOrder(
            instrument=instrument,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=max(spec.min_qty, spec.lot_size),
            time_in_force=TimeInForce.GTC,
            client_order_id=f"hedge-{uuid4().hex[:16]}",
        )
        with pytest.raises((PlatformError, InvalidOrderError)):
            await adapter.place_order(order)
    finally:
        try:
            if previous_mode is not None and previous_mode is not PositionMode.HEDGE:
                with contextlib.suppress(Exception):
                    await adapter.set_position_mode(previous_mode)
        finally:
            await adapter.disconnect()
