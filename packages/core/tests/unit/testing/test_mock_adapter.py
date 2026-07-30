"""Unit tests for MockAdapter — Section 11.1 testing surface."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.errors import (
    InvalidSymbolError,
    OrderNotFoundError,
    RateLimitError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import (
    BalanceUpdateEvent,
    ConnectionStateEvent,
    EventBus,
    FillEvent,
    PositionUpdateEvent,
)
from unified_trading_execution.testing import MockAdapter
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


def make_inst(symbol="BTC"):
    return Instrument(
        symbol=symbol,
        quote_currency="USDT",
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def make_order(**kwargs):
    defaults: dict = {
        "instrument": make_inst(),
        "order_type": OrderType.LIMIT,
        "side": OrderSide.BUY,
        "quantity": Decimal("0.001"),
        "time_in_force": TimeInForce.GTC,
        "price": Decimal("50000"),
        "client_order_id": "test-001",
    }
    defaults.update(kwargs)
    return UnifiedOrder(**defaults)


def make_result(status=OrderStatus.OPEN, **kwargs):
    defaults: dict = {
        "client_order_id": "test-001",
        "platform_order_id": "plat-001",
        "status": status,
        "filled_quantity": Decimal("0"),
        "average_fill_price": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    defaults.update(kwargs)
    return OrderResult(**defaults)


def make_fill():
    return FillRecord(
        client_order_id="test-001",
        platform_fill_id="fill-001",
        instrument=make_inst(),
        fill_quantity=Decimal("0.001"),
        fill_price=Decimal("50000"),
        fill_timestamp=NOW,
        fee_currency="USDT",
        fee_amount=Decimal("0.05"),
        correlation_id="corr-1",
    )


# ============================================================
# ABC compliance
# ============================================================


class TestABCCompliance:
    """MockAdapter must implement every abstract member of Adapter."""

    def test_can_instantiate(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        assert isinstance(mock.platform_name, str)
        assert isinstance(mock.account_id, str)

    def test_all_abstract_methods_implemented(self):
        """No TypeError when all methods are called."""
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        # Check all methods exist and are callable
        assert callable(mock.connect)
        assert callable(mock.disconnect)
        assert callable(mock.place_order)
        assert callable(mock.modify_order)
        assert callable(mock.cancel_order)
        assert callable(mock.get_order_by_client_id)
        assert callable(mock.fetch_instrument_spec)
        assert callable(mock.supported_order_types)
        assert callable(mock.get_rate_limits)
        # Properties
        assert isinstance(mock.is_connected, bool)
        assert isinstance(mock.platform_name, str)
        assert isinstance(mock.account_id, str)


# ============================================================
# Connection lifecycle
# ============================================================


class TestConnectionLifecycle:
    @pytest.mark.asyncio
    async def test_connect_publishes_event(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        events: list[ConnectionStateEvent] = []
        bus.subscribe(ConnectionStateEvent, lambda e: events.append(e))  # type: ignore[arg-type]
        await mock.connect()
        assert mock.is_connected is True
        assert len(events) == 1
        assert events[0].connected is True

    @pytest.mark.asyncio
    async def test_disconnect_publishes_event(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        await mock.connect()
        events: list[ConnectionStateEvent] = []
        bus.subscribe(ConnectionStateEvent, lambda e: events.append(e))  # type: ignore[arg-type]
        await mock.disconnect()
        assert mock.is_connected is False
        assert len(events) == 1
        assert events[0].connected is False

    @pytest.mark.asyncio
    async def test_connect_with_injected_error(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.set_next_error(RateLimitError("nope"))
        with pytest.raises(RateLimitError, match="nope"):
            await mock.connect()
        assert mock.is_connected is False

    @pytest.mark.asyncio
    async def test_set_connected_force(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.set_connected(True)
        assert mock.is_connected is True
        mock.set_connected(False)
        assert mock.is_connected is False

    @pytest.mark.asyncio
    async def test_disconnect_reconnect_cycle_fires_events(self):
        """Simulate a real mid-session drop-and-recover — Section 6.1/11.3."""
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        events: list[ConnectionStateEvent] = []
        bus.subscribe(ConnectionStateEvent, lambda e: events.append(e))  # type: ignore[arg-type]
        await mock.connect()  # connected=True
        await mock.disconnect()  # connected=False
        await mock.connect()  # reconnected=True
        assert len(events) == 3
        assert [e.connected for e in events] == [True, False, True]


# ============================================================
# Order operations — scriptable responses
# ============================================================


class TestPlaceOrder:
    @pytest.mark.asyncio
    async def test_queued_response_is_consumed(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        expected = make_result(status=OrderStatus.OPEN)
        mock.queue_place_order_response(expected)
        result = await mock.place_order(make_order())
        assert result is expected

    @pytest.mark.asyncio
    async def test_queued_exception_is_raised(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.queue_place_order_response(RateLimitError("throttled"))
        with pytest.raises(RateLimitError, match="throttled"):
            await mock.place_order(make_order())

    @pytest.mark.asyncio
    async def test_default_behavior_when_queue_empty(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        result = await mock.place_order(make_order())
        assert result.status == OrderStatus.OPEN
        assert result.platform_order_id is not None

    @pytest.mark.asyncio
    async def test_order_is_recorded_in_internal_book(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        result = await mock.place_order(make_order(client_order_id="abc"))
        assert "abc" in mock.orders
        assert mock.orders["abc"].status == OrderStatus.OPEN

    @pytest.mark.asyncio
    async def test_next_error_consumed_once(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.set_next_error(RateLimitError("once"))
        with pytest.raises(RateLimitError):
            await mock.place_order(make_order(client_order_id="a"))
        # Second call succeeds
        result = await mock.place_order(make_order(client_order_id="b"))
        assert result.status == OrderStatus.OPEN

    @pytest.mark.asyncio
    async def test_multiple_queued_responses_in_order(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        r1 = make_result(client_order_id="first")
        r2 = make_result(client_order_id="second")
        mock.queue_place_order_response(r1)
        mock.queue_place_order_response(r2)
        assert (await mock.place_order(make_order())).client_order_id == "first"
        assert (await mock.place_order(make_order())).client_order_id == "second"


class TestModifyOrder:
    @pytest.mark.asyncio
    async def test_queued_response_is_consumed(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        await mock.place_order(make_order(client_order_id="abc"))
        expected = make_result(status=OrderStatus.OPEN)
        mock.queue_modify_order_response(expected)
        result = await mock.modify_order(OrderModification("abc", price=Decimal("51000")))
        assert result is expected

    @pytest.mark.asyncio
    async def test_raises_order_not_found_for_unknown_id(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        with pytest.raises(OrderNotFoundError):
            await mock.modify_order(OrderModification("nonexistent", price=Decimal("51000")))

    @pytest.mark.asyncio
    async def test_queued_exception_overrides_not_found(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.queue_modify_order_response(RateLimitError("throttled"))
        # Modify order for unknown ID still raises queued error, not OrderNotFoundError
        with pytest.raises(RateLimitError, match="throttled"):
            await mock.modify_order(OrderModification("nonexistent", price=Decimal("51000")))

    @pytest.mark.asyncio
    async def test_default_modifies_order_in_book(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        await mock.place_order(make_order(client_order_id="abc", price=Decimal("50000")))
        result = await mock.modify_order(OrderModification("abc", price=Decimal("51000")))
        assert result.client_order_id == "abc"
        assert mock.orders["abc"].price == Decimal("51000")


class TestCancelOrder:
    @pytest.mark.asyncio
    async def test_queued_response_is_consumed(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.seed_order(
            OrderRecord(
                instrument=make_inst(),
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                time_in_force=TimeInForce.GTC,
                client_order_id="abc",
                price=Decimal("50000"),
                stop_price=None,
                reduce_only=False,
                client_tag=None,
                take_profit=None,
                stop_loss=None,
                platform_order_id="plat-1",
                status=OrderStatus.OPEN,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                correlation_id="corr-1",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        expected = make_result(status=OrderStatus.CANCELLED)
        mock.queue_cancel_order_response(expected)
        result = await mock.cancel_order("abc")
        assert result is expected

    @pytest.mark.asyncio
    async def test_raises_order_not_found_for_unknown_id(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        with pytest.raises(OrderNotFoundError):
            await mock.cancel_order("nonexistent")

    @pytest.mark.asyncio
    async def test_default_cancels_order_in_book(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        await mock.place_order(make_order(client_order_id="abc"))
        result = await mock.cancel_order("abc")
        assert result.status == OrderStatus.CANCELLED
        assert mock.orders["abc"].status == OrderStatus.CANCELLED


class TestGetOrderByClientId:
    @pytest.mark.asyncio
    async def test_queued_response_is_consumed(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        expected = make_result()
        mock.queue_get_order_response(expected)
        result = await mock.get_order_by_client_id("abc")
        assert result is expected

    @pytest.mark.asyncio
    async def test_default_returns_none_for_unknown(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        result = await mock.get_order_by_client_id("unknown")
        assert result is None

    @pytest.mark.asyncio
    async def test_default_returns_seeded_order(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.seed_order(
            OrderRecord(
                instrument=make_inst(),
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                time_in_force=TimeInForce.GTC,
                client_order_id="seeded",
                price=Decimal("50000"),
                stop_price=None,
                reduce_only=False,
                client_tag=None,
                take_profit=None,
                stop_loss=None,
                platform_order_id="plat-seed",
                status=OrderStatus.OPEN,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                correlation_id="corr-1",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        result = await mock.get_order_by_client_id("seeded")
        assert result is not None
        assert result.client_order_id == "seeded"

    @pytest.mark.asyncio
    async def test_queued_none_overrides_seeded_order(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.seed_order(
            OrderRecord(
                instrument=make_inst(),
                order_type=OrderType.LIMIT,
                side=OrderSide.BUY,
                quantity=Decimal("0.001"),
                time_in_force=TimeInForce.GTC,
                client_order_id="seeded",
                price=Decimal("50000"),
                stop_price=None,
                reduce_only=False,
                client_tag=None,
                take_profit=None,
                stop_loss=None,
                platform_order_id="plat-seed",
                status=OrderStatus.OPEN,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                correlation_id="corr-1",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        mock.queue_get_order_response(None)
        result = await mock.get_order_by_client_id("seeded")
        assert result is None


# ============================================================
# Event injection
# ============================================================


class TestEventInjection:
    def test_inject_fill_publishes_fill_event(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        fills: list[FillEvent] = []
        bus.subscribe(FillEvent, lambda e: fills.append(e))  # type: ignore[arg-type]
        fill = make_fill()
        mock.inject_fill(fill)
        assert len(fills) == 1
        assert fills[0].fill is fill

    def test_inject_position_update_publishes_event(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        events: list[PositionUpdateEvent] = []
        bus.subscribe(PositionUpdateEvent, lambda e: events.append(e))  # type: ignore[arg-type]
        pos = Position(
            instrument=make_inst(),
            quantity=Decimal("0.5"),
            average_entry_price=Decimal("50000"),
            updated_at=NOW,
        )
        mock.inject_position_update(pos)
        assert len(events) == 1
        assert events[0].position is pos

    def test_inject_balance_update_publishes_event(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        events: list[BalanceUpdateEvent] = []
        bus.subscribe(BalanceUpdateEvent, lambda e: events.append(e))  # type: ignore[arg-type]
        bal = Balance(
            currency="USDT",
            free=Decimal("9000"),
            used=Decimal("1000"),
            total=Decimal("10000"),
            updated_at=NOW,
        )
        mock.inject_balance_update(bal)
        assert len(events) == 1
        assert events[0].balance is bal


# ============================================================
# Instrument metadata
# ============================================================


class TestInstrumentSpec:
    @pytest.mark.asyncio
    async def test_seeded_spec_is_returned(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        inst = make_inst()
        spec = InstrumentSpec(
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            max_qty=Decimal("100"),
            min_notional=Decimal("10"),
            price_precision=2,
            qty_precision=3,
        )
        mock.add_instrument_spec(inst, spec)
        result = await mock.fetch_instrument_spec(inst)
        assert result is spec

    @pytest.mark.asyncio
    async def test_unseeded_instrument_raises_invalid_symbol(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        with pytest.raises(InvalidSymbolError):
            await mock.fetch_instrument_spec(make_inst("ETH"))


# ============================================================
# Capability and rate-limit configuration
# ============================================================


class TestCapabilities:
    def test_default_supported_order_types(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        types = mock.supported_order_types()
        assert types == frozenset(
            {OrderType.MARKET, OrderType.LIMIT, OrderType.STOP, OrderType.STOP_LIMIT}
        )

    def test_custom_supported_order_types(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        custom = frozenset({OrderType.MARKET, OrderType.LIMIT})
        mock.set_supported_order_types(custom)
        assert mock.supported_order_types() == custom

    @pytest.mark.asyncio
    async def test_default_rate_limits(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        rl = await mock.get_rate_limits()
        assert rl.requests_per_interval == 100
        assert rl.interval_seconds == 60.0
        assert rl.remaining == 100

    @pytest.mark.asyncio
    async def test_custom_rate_limits(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        rl = RateLimits(requests_per_interval=10, interval_seconds=1.0, remaining=5, reset_at=NOW)
        mock.set_rate_limits(rl)
        assert await mock.get_rate_limits() is rl


# ============================================================
# Error injection — single-shot, cross-method
# ============================================================


class TestErrorInjection:
    @pytest.mark.asyncio
    async def test_set_next_error_affects_place_order(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.set_next_error(UnsupportedOrderTypeError("no OCO"))
        with pytest.raises(UnsupportedOrderTypeError):
            await mock.place_order(make_order())

    @pytest.mark.asyncio
    async def test_set_next_error_affects_get_rate_limits(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.set_next_error(RateLimitError("over limit"))
        with pytest.raises(RateLimitError):
            await mock.get_rate_limits()

    @pytest.mark.asyncio
    async def test_set_next_error_affects_fetch_instrument_spec(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.add_instrument_spec(
            make_inst(),
            InstrumentSpec(
                tick_size=Decimal("0.01"),
                lot_size=Decimal("0.001"),
                min_qty=Decimal("0.001"),
                max_qty=Decimal("100"),
                min_notional=Decimal("10"),
                price_precision=2,
                qty_precision=3,
            ),
        )
        mock.set_next_error(RateLimitError("over limit"))
        with pytest.raises(RateLimitError):
            await mock.fetch_instrument_spec(make_inst())

    def test_set_next_error_none_clears(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.set_next_error(RateLimitError("err"))
        mock.set_next_error(None)
        # No error — should not raise
        mock._consume_error()  # Returns None (consumed the None we set)

    @pytest.mark.asyncio
    async def test_can_simulate_timeout(self):
        """set_next_error accepts TimeoutError for idempotency retry testing."""
        import asyncio

        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        mock.set_next_error(TimeoutError("connection timed out"))
        with pytest.raises(asyncio.TimeoutError, match="connection timed out"):
            await mock.get_rate_limits()
        # Single-shot — next call succeeds
        rl = await mock.get_rate_limits()
        assert rl is not None


# ============================================================
# Order book seeding and introspection
# ============================================================


class TestOrderBookSeeding:
    def test_seed_order_adds_to_book(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        record = OrderRecord(
            instrument=make_inst(),
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.GTC,
            client_order_id="seeded",
            price=Decimal("50000"),
            stop_price=None,
            reduce_only=False,
            client_tag=None,
            take_profit=None,
            stop_loss=None,
            platform_order_id="plat-1",
            status=OrderStatus.FILLED,
            filled_quantity=Decimal("0.001"),
            average_fill_price=Decimal("50000"),
            correlation_id="corr-1",
            created_at=NOW,
            updated_at=NOW,
        )
        mock.seed_order(record)
        assert "seeded" in mock.orders
        assert mock.orders["seeded"].status == OrderStatus.FILLED

    def test_orders_returns_copy(self):
        bus = EventBus()
        mock = MockAdapter(event_bus=bus)
        record = OrderRecord(
            instrument=make_inst(),
            order_type=OrderType.LIMIT,
            side=OrderSide.BUY,
            quantity=Decimal("0.001"),
            time_in_force=TimeInForce.GTC,
            client_order_id="x",
            price=Decimal("50000"),
            stop_price=None,
            reduce_only=False,
            client_tag=None,
            take_profit=None,
            stop_loss=None,
            platform_order_id="plat-1",
            status=OrderStatus.OPEN,
            filled_quantity=Decimal("0"),
            average_fill_price=None,
            correlation_id="corr-1",
            created_at=NOW,
            updated_at=NOW,
        )
        mock.seed_order(record)
        copy = mock.orders
        copy["y"] = record  # mutate copy
        assert "y" not in mock.orders


# ============================================================
# Layering — MockAdapter is a core testing tool, not an adapter
# ============================================================


class TestLayering:
    def test_mock_adapter_does_not_import_state_store(self):
        import unified_trading_execution.testing as mod

        ns = vars(mod)
        assert "StateStore" not in ns
        assert "SQLiteStateStore" not in ns

    def test_mock_adapter_does_not_import_engine(self):
        import unified_trading_execution.testing as mod

        ns = vars(mod)
        assert "Engine" not in ns
        assert "dispatch" not in ns
