"""MT5Engine — single-object async API for MetaTrader 5 trading.

Usage::

    from unified_trading_execution.mt5 import MT5Engine, MT5Config

    engine = MT5Engine(MT5Config(
        login=12345678,
        password="hunter2",
        server="ICMarkets-Demo",
    ))
    await engine.connect()
    result = await engine.place_order(order)
    await engine.modify_position_tpsl("12345", take_profit=...)
    await engine.ashutdown()
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.engine import DEFAULT_RECONCILE_INTERVAL_SECONDS, Engine
from unified_trading_execution.events import EventBus
from unified_trading_execution.mt5.adapter import MT5Adapter
from unified_trading_execution.mt5.config import MT5Config
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderRecord,
    TpSlAttachment,
)
from unified_trading_execution.types.position import Balance, Position


class MT5Engine(Engine):
    """All-in-one async engine for MetaTrader 5.

    Inherits every method from :class:`Engine` (``place_order``,
    ``cancel_order``, ``reconcile``, history accessors, etc.) and adds
    MT5-specific methods directly — one object, one import, zero wiring.
    """

    _adapter: MT5Adapter

    def __init__(
        self,
        config: MT5Config | MT5Adapter,
        *,
        state_store: StateStore | None = None,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
        reconcile_interval_seconds: float | None = DEFAULT_RECONCILE_INTERVAL_SECONDS,
    ) -> None:
        adapter = config if isinstance(config, MT5Adapter) else MT5Adapter(config)
        super().__init__(
            adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
            risk_config=risk_config,
            halt_config=halt_config,
            reconcile_interval_seconds=reconcile_interval_seconds,
        )

    # ── Position TP/SL modification ───────────────────────────────────

    async def modify_position_tpsl(
        self,
        position_id: str,
        *,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Modify TP/SL on an existing position via ``TRADE_ACTION_SLTP``.

        *position_id* is the MT5 position ticket. At least one of
        *take_profit* or *stop_loss* must be provided.
        """
        await self._adapter.modify_position_tpsl(
            position_id,
            take_profit=take_profit,
            stop_loss=stop_loss,
        )

    # ── Snapshots / reads ─────────────────────────────────────────────

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        return await self._adapter.fetch_instrument_spec(instrument)

    async def resolve_instrument(self, platform_symbol: str) -> Instrument:
        """Discover the canonical ``Instrument`` for an MT5 ``platform_symbol``."""
        return await self._adapter.resolve_instrument(platform_symbol)

    async def get_rate_limits(self) -> RateLimits:
        return await self._adapter.get_rate_limits()

    async def fetch_positions(self) -> list[Position]:
        return await self._adapter.fetch_positions()

    async def fetch_balances(self) -> dict[str, Balance]:
        return await self._adapter.fetch_balances()

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        return await self._adapter.fetch_open_orders()

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        return await self._adapter.fetch_fills(since=since)
