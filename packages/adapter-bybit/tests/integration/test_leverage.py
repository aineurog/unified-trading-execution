"""Integration tests for Bybit leverage / margin-mode operations (Phases 3-6).

Connect to the real Bybit testnet.  Skipped (not failed) when credentials are
missing so CI and local checkouts without API keys stay green.

These tests mutate real leverage / margin-mode state on testnet symbols, so
they restore the original value after each scenario where practical.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest

from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.errors import LeverageDriftError, LeverageExceedsMaxError
from unified_trading_execution.bybit.margin import LeverageConfig, MarginMode
from unified_trading_execution.bybit.symbols import to_bybit_symbol
from unified_trading_execution.errors import InvalidSymbolError
from unified_trading_execution.events import EventBus
from unified_trading_execution.state.halt import HaltConfig, HaltStateMachine
from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.types.enums import OrderSide, OrderType
from unified_trading_execution.types.instrument import Instrument

from .helpers import (
    align_down_to_tick,
    build_unified_order,
    random_client_id,
    valid_price_from_spec,
    valid_qty_from_spec,
)


@pytest.fixture
async def leverage_store() -> AsyncIterator[SQLiteStateStore]:
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    yield store
    await store.close()


@pytest.fixture
async def store_connected_adapter(
    bybit_config: BybitConfig,
    event_bus: EventBus,
    leverage_store: SQLiteStateStore,
) -> AsyncIterator[BybitAdapter]:
    """A testnet-connected adapter wired to an in-memory state store."""
    adapter = BybitAdapter(bybit_config, event_bus=event_bus, state_store=leverage_store)
    await adapter.connect()
    yield adapter
    with contextlib.suppress(Exception):
        await adapter.disconnect()


async def _set_and_verify(
    adapter: BybitAdapter,
    instrument: Instrument,
    leverage: int,
) -> None:
    await adapter.set_leverage(instrument, leverage)
    assert await adapter.get_leverage(instrument) == leverage


async def _categorise(adapter: BybitAdapter, instrument: Instrument) -> str:
    return adapter._instrument_to_category(instrument)


async def _symbol(adapter: BybitAdapter, instrument: Instrument) -> str:
    return to_bybit_symbol(instrument)


async def _set_leverage_direct(
    adapter: BybitAdapter,
    instrument: Instrument,
    leverage: int,
) -> None:
    """Change leverage on the platform directly, bypassing intent persistence.

    This simulates a leverage change made out-of-band (Bybit UI / another
    client) — the adapter's stored intent is left untouched so any strict
    check / reconciliation sees a drift.
    """
    await adapter._run_request(
        adapter._session.set_leverage,
        category=await _categorise(adapter, instrument),
        symbol=await _symbol(adapter, instrument),
        buyLeverage=str(leverage),
        sellLeverage=str(leverage),
    )


async def _set_margin_mode_direct(
    adapter: BybitAdapter,
    set_margin_mode: str,
) -> None:
    """Switch account-wide margin mode directly, bypassing intent persistence.

    Simulates an out-of-band margin-mode change (Bybit UI / another client) so
    reconciliation sees a drift.
    """
    await adapter._run_request(
        adapter._session.set_margin_mode,
        setMarginMode=set_margin_mode,
    )


async def _spec_valid_qty(adapter: BybitAdapter, instrument: Instrument) -> Decimal:
    spec = await adapter.fetch_instrument_spec(instrument)
    qty = valid_qty_from_spec(spec)
    return qty if qty > 0 else Decimal("0.001")


async def _spec_valid_price(
    adapter: BybitAdapter,
    instrument: Instrument,
    reference: Decimal,
) -> Decimal:
    spec = await adapter.fetch_instrument_spec(instrument)
    price = valid_price_from_spec(spec, reference)
    return price if price and price > 0 else Decimal("1")


async def _spec_non_marketable_buy_price(
    adapter: BybitAdapter,
    instrument: Instrument,
    reference: Decimal,
) -> Decimal:
    """A tick-aligned limit-buy price ~50% below the market.

    A buy limit below the market never executes, so an order placed at this
    price exercises the full place -> cancel path without opening a position
    that would trip the open-position guard for later tests.
    """
    spec = await adapter.fetch_instrument_spec(instrument)
    tick = spec.tick_size if spec.tick_size > 0 else Decimal("0.01")
    price = align_down_to_tick(reference * Decimal("0.5"), tick)
    return price if price > 0 else tick


class TestSetGetLeverage:
    async def test_set_get_leverage_linear(
        self,
        store_connected_adapter: BybitAdapter,
        linear_instrument: Instrument,
    ) -> None:
        adapter = store_connected_adapter
        original = await adapter.get_leverage(linear_instrument)
        try:
            await _set_and_verify(adapter, linear_instrument, 10)
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await adapter.set_leverage(linear_instrument, original)

    async def test_set_get_leverage_inverse(
        self,
        store_connected_adapter: BybitAdapter,
        traded_instrument: Instrument,
    ) -> None:
        adapter = store_connected_adapter
        if adapter._instrument_to_category(traded_instrument) != "inverse":
            pytest.skip("no inverse instrument available")
        original = await adapter.get_leverage(traded_instrument)
        try:
            await _set_and_verify(adapter, traded_instrument, 5)
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await adapter.set_leverage(traded_instrument, original)

    async def test_set_leverage_spot_raises(
        self,
        store_connected_adapter: BybitAdapter,
        spot_instrument: Instrument,
    ) -> None:
        with pytest.raises(InvalidSymbolError):
            await store_connected_adapter.set_leverage(spot_instrument, 10)

    async def test_set_leverage_exceeds_max(
        self,
        store_connected_adapter: BybitAdapter,
        linear_instrument: Instrument,
    ) -> None:
        adapter = store_connected_adapter
        spec = await adapter.fetch_instrument_spec(linear_instrument)
        assert spec.max_leverage is not None
        with pytest.raises(LeverageExceedsMaxError):
            await adapter.set_leverage(linear_instrument, int(spec.max_leverage) + 1)

    async def test_leverage_symmetric(
        self,
        store_connected_adapter: BybitAdapter,
        linear_instrument: Instrument,
    ) -> None:
        """set_leverage sets both buy and sell leverage to the same value."""
        adapter = store_connected_adapter
        original = await adapter.get_leverage(linear_instrument)
        try:
            await _set_and_verify(adapter, linear_instrument, 25)
            # One-way mode: the position carries a single `leverage` field
            # (buyLeverage/sellLeverage only appear in hedge mode).
            data, _ = await adapter._run_request(
                adapter._session.get_positions,
                category=adapter._instrument_to_category(linear_instrument),
                symbol=f"{linear_instrument.symbol}{linear_instrument.quote_currency}",
            )
            entry = ((data.get("result") or {}).get("list") or [{}])[0]
            assert entry.get("leverage") == "25"
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await adapter.set_leverage(linear_instrument, original)


class TestMarginMode:
    async def test_set_margin_mode_cross_to_isolated(
        self,
        store_connected_adapter: BybitAdapter,
        linear_instrument: Instrument,
    ) -> None:
        adapter = store_connected_adapter
        original = await adapter.get_margin_mode(linear_instrument)
        try:
            await adapter.set_margin_mode(linear_instrument, MarginMode.ISOLATED, leverage=10)
            assert await adapter.get_margin_mode(linear_instrument) is MarginMode.ISOLATED
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await adapter.set_margin_mode(linear_instrument, original, leverage=10)

    async def test_set_margin_mode_isolated_to_cross(
        self,
        store_connected_adapter: BybitAdapter,
        linear_instrument: Instrument,
    ) -> None:
        adapter = store_connected_adapter
        original = await adapter.get_margin_mode(linear_instrument)
        try:
            await adapter.set_margin_mode(linear_instrument, MarginMode.CROSS, leverage=10)
            assert await adapter.get_margin_mode(linear_instrument) is MarginMode.CROSS
        finally:
            if original is not None and original is not MarginMode.CROSS:
                with contextlib.suppress(Exception):
                    await adapter.set_margin_mode(linear_instrument, original, leverage=10)


class TestReapplyOnConnect:
    async def test_leverage_reapply_on_connect(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """Set leverage, disconnect, reconnect, verify it was reapplied."""
        store = leverage_store
        adapter = BybitAdapter(bybit_config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_leverage(linear_instrument, 15)
            await adapter.disconnect()
            adapter2 = BybitAdapter(
                bybit_config,
                event_bus=EventBus(),
                state_store=store,
            )
            await adapter2.connect()
            assert await adapter2.get_leverage(linear_instrument) == 15
            await adapter2.disconnect()
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await adapter.set_leverage(linear_instrument, original)
                    await adapter.disconnect()

    async def test_leverage_apply_failed_on_delisted(
        self,
        bybit_config: BybitConfig,
        event_bus: EventBus,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """Seed stored intent for a non-existent symbol; connect must not crash."""
        store = leverage_store
        await store.set_adapter_config("leverage.DOESNOTEXISTUSDT", "10")
        adapter = BybitAdapter(bybit_config, event_bus=event_bus, state_store=store)
        await adapter.connect()
        assert adapter.is_connected is True
        await adapter.disconnect()


class TestStrictCheck:
    """Phase 5 (Step 10) — pre-order leverage verification on the live platform."""

    async def test_strict_check_blocks_drifted_order(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
        linear_reference_price: Decimal,
    ) -> None:
        """Set leverage via adapter, drift it out-of-band, order must be rejected."""
        store = leverage_store
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(strict_check=True, on_drift="notify"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_leverage(linear_instrument, 10)
            await _set_leverage_direct(adapter, linear_instrument, 50)

            qty = await _spec_valid_qty(adapter, linear_instrument)
            price = await _spec_valid_price(adapter, linear_instrument, linear_reference_price)
            order = build_unified_order(
                linear_instrument,
                OrderType.LIMIT,
                OrderSide.BUY,
                qty,
                client_order_id=random_client_id("sc"),
                price=price,
            )
            with pytest.raises(LeverageDriftError):
                await adapter.place_order(order)
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await _set_leverage_direct(adapter, linear_instrument, original)
            await adapter.disconnect()

    async def test_strict_check_off_skips_verification(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
        linear_reference_price: Decimal,
    ) -> None:
        """With strict_check off, a drifted order proceeds (no per-order query).

        The buy limit is priced ~50% below the market so it rests unfilled —
        place succeeds (order id returned) but never opens a position that
        would trip the open-position guard for later scenarios.
        """
        store = leverage_store
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(strict_check=False),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original = await adapter.get_leverage(linear_instrument)
        order = None
        try:
            await adapter.set_leverage(linear_instrument, 10)
            await _set_leverage_direct(adapter, linear_instrument, 50)

            spec = await adapter.fetch_instrument_spec(linear_instrument)
            price = await _spec_non_marketable_buy_price(
                adapter, linear_instrument, linear_reference_price
            )
            qty = valid_qty_from_spec(spec, price)
            order = build_unified_order(
                linear_instrument,
                OrderType.LIMIT,
                OrderSide.BUY,
                qty,
                client_order_id=random_client_id("scoff"),
                price=price,
            )
            result = await adapter.place_order(order)
            assert result.platform_order_id is not None
        finally:
            if order is not None and order.client_order_id is not None:
                with contextlib.suppress(Exception):
                    await adapter.cancel_order(order.client_order_id)
            if original is not None:
                with contextlib.suppress(Exception):
                    await _set_leverage_direct(adapter, linear_instrument, original)
            await adapter.disconnect()


class TestReconcileIntent:
    """Phase 6 (Step 11 / Step 14) — adapter-owned user-intent reconciliation."""

    async def test_leverage_drift_detected_and_reapplied(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """Drift the platform out-of-band, reconcile, verify intent restored."""
        store = leverage_store
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(on_drift="reapply"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_leverage(linear_instrument, 10)
            await _set_leverage_direct(adapter, linear_instrument, 50)
            assert await adapter.get_leverage(linear_instrument) == 50

            await adapter.reconcile_user_intent()

            assert await adapter.get_leverage(linear_instrument) == 10
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await _set_leverage_direct(adapter, linear_instrument, original)
            await adapter.disconnect()

    async def test_leverage_drift_halt_mode(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """on_drift=halt: a drifted instrument enters a halt on reconcile."""
        store = leverage_store
        halt_machine = HaltStateMachine(HaltConfig(auto_halt_enabled=True))
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(on_drift="halt"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        adapter.attach_halt_machine(halt_machine)
        await adapter.connect()
        original = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_leverage(linear_instrument, 10)
            await _set_leverage_direct(adapter, linear_instrument, 50)

            await adapter.reconcile_user_intent()

            assert len(halt_machine.active_halts()) == 1
            halted = halt_machine.active_halts()[0]
            assert halted.instrument is not None
            assert halted.instrument.symbol == linear_instrument.symbol
        finally:
            with contextlib.suppress(Exception):
                halt_machine.try_clear_halt(
                    "instrument",
                    instrument=linear_instrument,
                    reconciliation_is_clean=True,
                )
            if original is not None:
                with contextlib.suppress(Exception):
                    await _set_leverage_direct(adapter, linear_instrument, original)
            await adapter.disconnect()

    async def test_leverage_drift_notify_mode(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """on_drift=notify: drift is reported but the platform is left alone."""
        store = leverage_store
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(on_drift="notify"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_leverage(linear_instrument, 10)
            await _set_leverage_direct(adapter, linear_instrument, 50)

            await adapter.reconcile_user_intent()

            assert await adapter.get_leverage(linear_instrument) == 50
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await _set_leverage_direct(adapter, linear_instrument, original)
            await adapter.disconnect()

    async def test_recovery_clears_drift_halt(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """A clean reconcile clears the residual leverage-drift halt."""
        store = leverage_store
        halt_machine = HaltStateMachine(HaltConfig(auto_halt_enabled=True))
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(on_drift="halt"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        adapter.attach_halt_machine(halt_machine)
        await adapter.connect()
        original = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_leverage(linear_instrument, 10)
            await _set_leverage_direct(adapter, linear_instrument, 50)

            await adapter.reconcile_user_intent()
            assert len(halt_machine.active_halts()) == 1

            await _set_leverage_direct(adapter, linear_instrument, 10)
            await adapter.reconcile_user_intent()
            assert halt_machine.active_halts() == []
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await _set_leverage_direct(adapter, linear_instrument, original)
            await adapter.disconnect()


class TestReconcileMarginMode:
    """Phase 6 (Step 14) — margin-mode drift reconciliation on the live platform.

    Margin mode is account-wide, so "drift" here means the account-wide
    ``set-margin-mode`` was changed out-of-band; reconciliation must restore the
    stored per-symbol intent.
    """

    async def test_margin_mode_drift_detected_and_reapplied(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """Drift the account out of isolated out-of-band; reconcile restores it."""
        store = leverage_store
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(on_drift="reapply"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original_mode = await adapter.get_margin_mode(linear_instrument)
        original_leverage = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_margin_mode(linear_instrument, MarginMode.ISOLATED, leverage=10)
            await _set_margin_mode_direct(adapter, "REGULAR_MARGIN")
            assert await adapter.get_margin_mode(linear_instrument) is MarginMode.CROSS

            await adapter.reconcile_user_intent()

            assert await adapter.get_margin_mode(linear_instrument) is MarginMode.ISOLATED
        finally:
            if original_leverage is not None:
                with contextlib.suppress(Exception):
                    await _set_leverage_direct(adapter, linear_instrument, original_leverage)
            if original_mode is not None:
                with contextlib.suppress(Exception):
                    await adapter.set_margin_mode(
                        linear_instrument,
                        original_mode,
                        leverage=original_leverage or 10,
                    )
            await adapter.disconnect()

    async def test_margin_mode_drift_preserves_platform_leverage_after_remove(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """remove_leverage leaves margin intent; margin drift reapply must keep
        the platform's current leverage rather than forcing 1x."""
        store = leverage_store
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(on_drift="reapply"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original_mode = await adapter.get_margin_mode(linear_instrument)
        original_leverage = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_margin_mode(linear_instrument, MarginMode.ISOLATED, leverage=10)
            await adapter.remove_leverage(linear_instrument)
            # Out-of-band: margin to cross AND leverage to 25, no stored leverage.
            await _set_margin_mode_direct(adapter, "REGULAR_MARGIN")
            await _set_leverage_direct(adapter, linear_instrument, 25)
            assert await adapter.get_margin_mode(linear_instrument) is MarginMode.CROSS
            assert await adapter.get_leverage(linear_instrument) == 25

            await adapter.reconcile_user_intent()

            # Margin reverted to isolated without forcing leverage back to 1x.
            assert await adapter.get_margin_mode(linear_instrument) is MarginMode.ISOLATED
            assert await adapter.get_leverage(linear_instrument) == 25
        finally:
            if original_leverage is not None:
                with contextlib.suppress(Exception):
                    await _set_leverage_direct(adapter, linear_instrument, original_leverage)
            if original_mode is not None:
                with contextlib.suppress(Exception):
                    await adapter.set_margin_mode(
                        linear_instrument,
                        original_mode,
                        leverage=original_leverage or 10,
                    )
            await adapter.disconnect()


class TestAuditTrail:
    """Phase 7 (Step 13) — leverage events appear in the audit trail."""

    async def test_leverage_events_in_audit_trail(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """Reapply-on-connect and drift-reconcile both land in the audit trail."""
        store = leverage_store
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(on_drift="reapply"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original = await adapter.get_leverage(linear_instrument)
        try:
            await adapter.set_leverage(linear_instrument, 10)
            await adapter.disconnect()

            # Reconnect: stored intent reapplied → LeverageAppliedEvent + audit.
            adapter2 = BybitAdapter(config, event_bus=EventBus(), state_store=store)
            await adapter2.connect()
            assert await adapter2.get_leverage(linear_instrument) == 10

            applied = [
                e
                for e in await store.query_audit_events()
                if e.event_type == "bybit.leverage.applied"
            ]
            assert len(applied) == 1
            assert applied[0].payload["symbol"] == to_bybit_symbol(linear_instrument)
            assert applied[0].payload["leverage"] == 10

            # Drift out-of-band, reconcile → LeverageDriftEvent + audit.
            await _set_leverage_direct(adapter2, linear_instrument, 50)
            await adapter2.reconcile_user_intent()
            assert await adapter2.get_leverage(linear_instrument) == 10

            drift = [
                e
                for e in await store.query_audit_events()
                if e.event_type == "bybit.leverage.drift"
            ]
            assert len(drift) == 1
            assert drift[0].payload["symbol"] == to_bybit_symbol(linear_instrument)
            assert drift[0].payload["stored_leverage"] == 10
            assert drift[0].payload["platform_leverage"] == 50
            assert drift[0].payload["action_taken"] == "reapplied"
        finally:
            with contextlib.suppress(Exception):
                if original is not None:
                    await _set_leverage_direct(adapter2, linear_instrument, original)
            with contextlib.suppress(Exception):
                await adapter.disconnect()
            with contextlib.suppress(Exception):
                await adapter2.disconnect()

    async def test_margin_mode_events_in_audit_trail(
        self,
        bybit_config: BybitConfig,
        linear_instrument: Instrument,
        leverage_store: SQLiteStateStore,
    ) -> None:
        """Margin-mode reapply-on-connect lands in the audit trail."""
        store = leverage_store
        config = BybitConfig(
            api_key=bybit_config.api_key,
            api_secret=bybit_config.api_secret,
            testnet=True,
            leverage=LeverageConfig(on_drift="reapply"),
        )
        adapter = BybitAdapter(config, event_bus=EventBus(), state_store=store)
        await adapter.connect()
        original = await adapter.get_margin_mode(linear_instrument)
        adapter2 = None
        try:
            await adapter.set_margin_mode(linear_instrument, MarginMode.ISOLATED, leverage=10)
            await adapter.disconnect()

            adapter2 = BybitAdapter(config, event_bus=EventBus(), state_store=store)
            await adapter2.connect()
            assert await adapter2.get_margin_mode(linear_instrument) is MarginMode.ISOLATED

            changed = [
                e
                for e in await store.query_audit_events()
                if e.event_type == "bybit.margin_mode.changed"
            ]
            assert len(changed) == 1
            assert changed[0].payload["symbol"] == to_bybit_symbol(linear_instrument)
            assert changed[0].payload["current"] == "isolated"
            assert changed[0].payload["leverage"] == 10
        finally:
            if original is not None:
                with contextlib.suppress(Exception):
                    await adapter.set_margin_mode(linear_instrument, original, leverage=10)
            if adapter2 is not None:
                with contextlib.suppress(Exception):
                    await adapter2.disconnect()
            with contextlib.suppress(Exception):
                await adapter.disconnect()
