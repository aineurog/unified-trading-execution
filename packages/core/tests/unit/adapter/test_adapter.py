"""Unit tests for the Adapter ABC — Section 17.10."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime

import pytest

from unified_trading_execution.types.enums import OrderType


class TestAdapterABCSurface:
    """Verify the ABC declares exactly the method surface from Section 17.10."""

    @pytest.fixture
    def abc(self):
        from unified_trading_execution.adapter import Adapter

        return Adapter

    @pytest.fixture
    def abstract_methods(self, abc):
        return {
            name
            for name, member in inspect.getmembers(abc)
            if getattr(member, "__isabstractmethod__", False)
        }

    # ---- Identification ----

    def test_platform_name_is_abstract_property(self, abstract_methods):
        assert "platform_name" in abstract_methods

    def test_account_id_is_abstract_property(self, abstract_methods):
        assert "account_id" in abstract_methods

    # ---- Connection lifecycle ----

    def test_connect_is_abstract_coroutine(self, abstract_methods):
        assert "connect" in abstract_methods

    def test_disconnect_is_abstract_coroutine(self, abstract_methods):
        assert "disconnect" in abstract_methods

    def test_is_connected_is_abstract_property(self, abstract_methods):
        assert "is_connected" in abstract_methods

    # ---- Order operations ----

    def test_place_order_is_abstract_coroutine(self, abstract_methods):
        assert "place_order" in abstract_methods

    def test_modify_order_is_abstract_coroutine(self, abstract_methods):
        assert "modify_order" in abstract_methods

    def test_cancel_order_is_abstract_coroutine(self, abstract_methods):
        assert "cancel_order" in abstract_methods

    def test_get_order_by_client_id_is_abstract_coroutine(self, abstract_methods):
        assert "get_order_by_client_id" in abstract_methods

    # ---- Instrument metadata ----

    def test_fetch_instrument_spec_is_abstract_coroutine(self, abstract_methods):
        assert "fetch_instrument_spec" in abstract_methods

    # ---- Capability reporting ----

    def test_supported_order_types_is_abstract_sync(self, abstract_methods):
        assert "supported_order_types" in abstract_methods

    # ---- Rate limits ----

    def test_get_rate_limits_is_abstract_coroutine(self, abstract_methods):
        assert "get_rate_limits" in abstract_methods

    # ---- Exact surface ----

    EXPECTED_ABSTRACT_MEMBERS = frozenset(
        {
            "platform_name",
            "account_id",
            "connect",
            "disconnect",
            "is_connected",
            "place_order",
            "modify_order",
            "cancel_order",
            "get_order_by_client_id",
            "fetch_instrument_spec",
            "supported_order_types",
            "get_rate_limits",
        }
    )

    def test_exact_abstract_surface(self, abstract_methods, abc):
        """Exactly 12 abstract members — nothing added, nothing missing."""
        # Filter out dunder and non-public inherited abstract methods
        ours = {n for n in abstract_methods if not n.startswith("_")}
        assert ours == self.EXPECTED_ABSTRACT_MEMBERS, (
            f"Unexpected abstract members: {ours ^ self.EXPECTED_ABSTRACT_MEMBERS}"
        )


class TestSupportedOrderTypesMinimum:
    """supported_order_types() must always include the guaranteed core set."""

    def test_docstring_declares_minimum(self):
        from unified_trading_execution.adapter import Adapter

        doc = Adapter.supported_order_types.__doc__ or ""
        assert "MARKET" in doc
        assert "LIMIT" in doc
        assert "STOP" in doc
        assert "STOP_LIMIT" in doc
        assert "minimum" in doc.lower()

    def test_return_type_is_frozenset_of_ordertype(self):
        import typing

        from unified_trading_execution.adapter import Adapter

        hints = typing.get_type_hints(Adapter.supported_order_types)
        assert hints.get("return") == frozenset[OrderType]


class TestAdapterZeroImplementation:
    """The ABC must be pure interface — zero implementation, per Section 4."""

    def test_cannot_instantiate_directly(self):
        from unified_trading_execution.adapter import Adapter

        with pytest.raises(TypeError, match="abstract"):
            Adapter()  # type: ignore[abstract]

    def test_no_concrete_methods(self):
        """Public methods must be abstract, except the listed optional
        methods which provide defaults so adapters that don't need them
        don't have to implement them.
        """
        from unified_trading_execution.adapter import Adapter

        _ALLOWED_CONCRETE = frozenset(
            {
                "fetch_positions",
                "fetch_balances",
                "fetch_open_orders",
                "fetch_fills",
                "fetch_ticker",
                "attach_halt_machine",
                "attach_event_bus",
                "attach_state_store",
                "reconcile_user_intent",
                "modify_position_tpsl",
                "get_position_tpsl",
                "clear_position_tpsl",
                "fetch_candles",
                "place_trailing_stop",
                "cancel_trailing_stop",
                "get_trailing_stop",
                "position_protection_capability",
                "trailing_capability",
                "market_data_capability",
                "resolve_account_id",
            }
        )
        concrete = []
        for name, member in inspect.getmembers(Adapter, predicate=inspect.isfunction):
            if name.startswith("_"):
                continue
            if name in _ALLOWED_CONCRETE:
                continue
            if not getattr(member, "__isabstractmethod__", False):
                concrete.append(name)
        assert concrete == [], f"Found concrete methods on Adapter ABC: {concrete}"

    def test_module_has_no_standalone_functions(self):
        """No utility functions in the adapter module — pure interface + dataclass."""
        import unified_trading_execution.adapter as mod

        funcs = [
            name
            for name, obj in inspect.getmembers(mod, inspect.isfunction)
            if not name.startswith("_") and getattr(obj, "__module__", None) == mod.__name__
        ]
        assert funcs == []

    def test_adapter_never_imports_state_store(self):
        """Section 17.10: adapter never imports or depends on StateStore."""
        import unified_trading_execution.adapter as mod

        ns = vars(mod)
        assert "StateStore" not in ns
        assert "SQLiteStateStore" not in ns

    def test_adapter_never_imports_engine(self):
        """Adapters must not import from the Engine module."""
        import unified_trading_execution.adapter as mod

        ns = vars(mod)
        assert "Engine" not in ns
        assert "dispatch" not in ns


class TestRateLimits:
    """RateLimits dataclass — Section 17.10."""

    def test_constructs(self):
        from unified_trading_execution.adapter import RateLimits

        now = datetime.now(tz=UTC)
        rl = RateLimits(
            requests_per_interval=100,
            interval_seconds=60.0,
            remaining=50,
            reset_at=now,
        )
        assert rl.requests_per_interval == 100
        assert rl.interval_seconds == 60.0
        assert rl.remaining == 50
        assert rl.reset_at == now

    def test_is_frozen(self):
        from unified_trading_execution.adapter import RateLimits

        now = datetime.now(tz=UTC)
        rl = RateLimits(100, 60.0, 50, now)
        with pytest.raises(Exception):
            rl.remaining = 99  # type: ignore[misc]

    def test_slots_no_dict(self):
        from unified_trading_execution.adapter import RateLimits

        now = datetime.now(tz=UTC)
        rl = RateLimits(100, 60.0, 50, now)
        assert not hasattr(rl, "__dict__")


class TestDefaultCapabilities:
    """Optional capability queries report nothing supported by default."""

    @pytest.fixture
    def adapter(self):
        from unified_trading_execution.events import EventBus
        from unified_trading_execution.testing import MockAdapter

        return MockAdapter(event_bus=EventBus())

    @pytest.fixture
    def instrument(self):
        from unified_trading_execution.types.enums import AssetClass
        from unified_trading_execution.types.instrument import Instrument

        return Instrument(symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT)

    def test_position_protection_is_unsupported_by_default(self, adapter, instrument):
        cap = adapter.position_protection_capability(instrument)
        assert cap.supported is False
        assert cap.can_set_stop is False
        assert cap.can_set_take_profit is False
        assert cap.can_clear_stop is False
        assert cap.can_clear_take_profit is False
        assert cap.preserves_unmentioned_side is False
        assert cap.trigger_bases == frozenset()
        assert cap.min_reprice_interval_seconds == 0.0
        assert cap.reprice_gap is False

    def test_native_trailing_is_unsupported_by_default(self, adapter, instrument):
        cap = adapter.trailing_capability(instrument)
        assert cap.supported is False
        assert cap.offset_types == frozenset()
        assert cap.bases == frozenset()
        assert cap.activations == frozenset()
        assert cap.supports_step is False
        assert cap.supports_initial_stop is False
        assert cap.supports_take_profit is False
        assert cap.position_scoped is False
        assert cap.min_reprice_interval_seconds == 0.0

    def test_market_data_is_empty_by_default(self, adapter, instrument):
        cap = adapter.market_data_capability(instrument)
        assert cap.ticker_bases == frozenset()
        assert cap.min_ticker_interval_seconds == 0.0
        assert cap.candle_timeframes == frozenset()
        assert cap.streaming_ticker is False

    def test_capabilities_are_frozen_and_slotted(self, adapter, instrument):
        cap = adapter.trailing_capability(instrument)
        with pytest.raises(AttributeError):
            cap.supported = True  # type: ignore[misc]
        assert not hasattr(cap, "__dict__")


class TestOptionalMethodDefaults:
    """New optional methods raise NotImplementedError unless overridden."""

    @pytest.fixture
    def adapter(self):
        from unified_trading_execution.events import EventBus
        from unified_trading_execution.testing import MockAdapter

        return MockAdapter(event_bus=EventBus())

    @pytest.fixture
    def instrument(self):
        from unified_trading_execution.types.enums import AssetClass
        from unified_trading_execution.types.instrument import Instrument

        return Instrument(symbol="BTC", quote_currency="USDT", asset_class=AssetClass.SPOT)

    async def test_clear_position_tpsl_requires_a_named_side(self, adapter, instrument):
        with pytest.raises(ValueError, match="at least one"):
            await adapter.clear_position_tpsl(instrument, "leg-1")

    async def test_clear_position_tpsl_is_unsupported_by_default(self, adapter, instrument):
        with pytest.raises(NotImplementedError, match="clearing TP/SL"):
            await adapter.clear_position_tpsl(instrument, "leg-1", stop_loss=True)

    async def test_fetch_candles_is_unsupported_by_default(self, adapter, instrument):
        from unified_trading_execution.trailing.types import CandleTimeframe

        with pytest.raises(NotImplementedError, match="fetching candles"):
            await adapter.fetch_candles(instrument, CandleTimeframe.H1, limit=20)

    async def test_place_trailing_stop_is_unsupported_by_default(self, adapter, instrument):
        from decimal import Decimal

        from unified_trading_execution.trailing.types import (
            NativeTrailingRequest,
            TrailingOffsetType,
        )
        from unified_trading_execution.types.enums import OrderSide

        request = NativeTrailingRequest(
            instrument=instrument,
            position_id="leg-1",
            side=OrderSide.SELL,
            quantity=Decimal("1"),
            offset=Decimal("2"),
            offset_type=TrailingOffsetType.PRICE,
            basis=None,
            activation_price=None,
            client_id="tr-1",
        )
        with pytest.raises(NotImplementedError, match="native trailing"):
            await adapter.place_trailing_stop(request)

    async def test_cancel_and_get_trailing_stop_are_unsupported_by_default(
        self, adapter, instrument
    ):
        with pytest.raises(NotImplementedError, match="native trailing"):
            await adapter.cancel_trailing_stop("n1", instrument)
        with pytest.raises(NotImplementedError, match="native trailing"):
            await adapter.get_trailing_stop("n1", instrument)
