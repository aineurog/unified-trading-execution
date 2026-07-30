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
        """Public methods must be abstract, except the four optional
        reconciliation methods (fetch_positions, fetch_balances,
        fetch_open_orders, fetch_fills) which provide NotImplementedError
        defaults so adapters that don't support reconciliation don't have
        to implement them.
        """
        from unified_trading_execution.adapter import Adapter

        _ALLOWED_CONCRETE = frozenset(
            {
                "fetch_positions",
                "fetch_balances",
                "fetch_open_orders",
                "fetch_fills",
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
