"""SyncBybitEngine — single-object blocking API for Bybit trading.

Usage::

    from unified_trading_execution.bybit import SyncBybitEngine, BybitConfig

    engine = SyncBybitEngine(BybitConfig(api_key="...", api_secret="..."))
    engine.connect()
    engine.set_leverage(btc, buy_leverage=10)
    result = engine.place_order(order)
    engine.shutdown()
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Literal

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.config import BybitConfig
from unified_trading_execution.bybit.enums import MarginMode, PositionMode
from unified_trading_execution.events import EventBus
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position


class SyncBybitEngine(SyncEngine):
    """All-in-one blocking engine for Bybit.

    Inherits every method from :class:`SyncEngine` (``place_order``,
    ``cancel_order``, ``reconcile``, history accessors, etc.) and adds
    Bybit-specific methods directly — one object, one import, zero wiring.
    """

    def __init__(
        self,
        config: BybitConfig | BybitAdapter,
        *,
        state_store: StateStore | None = None,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
        reconcile_interval_seconds: float | None = None,
    ) -> None:
        adapter = config if isinstance(config, BybitAdapter) else BybitAdapter(config)
        super().__init__(
            adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
            risk_config=risk_config,
            halt_config=halt_config,
            reconcile_interval_seconds=reconcile_interval_seconds,
        )

    @property
    def _adapter(self) -> BybitAdapter:
        adapter = self.adapter
        assert isinstance(adapter, BybitAdapter)
        return adapter

    # ── leverage intent ───────────────────────────────────────────────

    def set_leverage(
        self,
        instrument: Instrument,
        *,
        buy_leverage: int = 1,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        strict_check: bool = True,
        block_on_open_position: bool = True,
        auto_apply_on_connect: bool = True,
    ) -> None:
        self._run(
            self._adapter.set_leverage(
                instrument,
                buy_leverage=buy_leverage,
                on_drift=on_drift,
                strict_check=strict_check,
                block_on_open_position=block_on_open_position,
                auto_apply_on_connect=auto_apply_on_connect,
            )
        )

    def get_leverage(self, instrument: Instrument) -> tuple[int, int] | None:
        return self._run(self._adapter.get_leverage(instrument))

    def remove_leverage(self, instrument: Instrument) -> None:
        self._run(self._adapter.remove_leverage(instrument))

    # ── position mode intent ───────────────────────────────────────────

    def set_position_mode(
        self,
        instrument: Instrument,
        mode: str | PositionMode = "one_way",
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        self._run(
            self._adapter.set_position_mode(
                instrument,
                mode,
                on_drift=on_drift,
                auto_apply_on_connect=auto_apply_on_connect,
            )
        )

    def get_position_mode(self, instrument: Instrument) -> PositionMode | None:
        return self._run(self._adapter.get_position_mode(instrument))

    def remove_position_mode(self, instrument: Instrument) -> None:
        self._run(self._adapter.remove_position_mode(instrument))

    def set_position_mode_for_coin(
        self,
        coin: str,
        category: str,
        mode: str | PositionMode = "one_way",
    ) -> None:
        self._run(self._adapter.set_position_mode_for_coin(coin, category, mode))

    # ── margin mode ───────────────────────────────────────────────────

    def set_margin_mode(self, mode: str | MarginMode) -> None:
        self._run(self._adapter.set_margin_mode(MarginMode(mode)))

    def get_margin_mode(self) -> MarginMode | None:
        return self._run(self._adapter.get_margin_mode())

    # ── snapshots / reads ─────────────────────────────────────────────

    def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        return self._run(self._adapter.fetch_instrument_spec(instrument))

    def get_rate_limits(self) -> RateLimits:
        return self._run(self._adapter.get_rate_limits())

    def fetch_positions(self) -> dict[Instrument, Position]:
        return self._run(self._adapter.fetch_positions())

    def fetch_balances(self) -> dict[str, Balance]:
        return self._run(self._adapter.fetch_balances())

    def fetch_open_orders(self) -> dict[str, OrderRecord]:
        return self._run(self._adapter.fetch_open_orders())

    def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        return self._run(self._adapter.fetch_fills(since=since))

    def reconcile_user_intent(self) -> None:
        self._run(self._adapter.reconcile_user_intent())
