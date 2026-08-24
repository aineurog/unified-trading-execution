"""IBKREngine — single-object async API for Interactive Brokers trading.

Usage::

    from unified_trading_execution.ibkr import IBKREngine, IBKRConfig

    engine = IBKREngine(IBKRConfig(
        host="127.0.0.1",
        port=4002,
        client_id=1,
        account="DU123456",
    ))
    await engine.connect()
    result = await engine.place_order(order)
    await engine.ashutdown()
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.engine import Engine
from unified_trading_execution.events import EventBus
from unified_trading_execution.ibkr.adapter import IBKRAdapter
from unified_trading_execution.ibkr.config import IBKRConfig
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import FillRecord, OrderRecord
from unified_trading_execution.types.position import Balance, Position


class IBKREngine(Engine):
    """All-in-one async engine for Interactive Brokers.

    Inherits every method from :class:`Engine` (``place_order``,
    ``cancel_order``, ``reconcile``, history accessors, etc.) and adds
    IBKR-specific methods directly — one object, one import, zero wiring.
    """

    _adapter: IBKRAdapter

    def __init__(
        self,
        config: IBKRConfig | IBKRAdapter,
        *,
        state_store: StateStore | None = None,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
    ) -> None:
        adapter = config if isinstance(config, IBKRAdapter) else IBKRAdapter(config)
        super().__init__(
            adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
            risk_config=risk_config,
            halt_config=halt_config,
        )

    # ── Snapshots / reads ─────────────────────────────────────────────

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        return await self._adapter.fetch_instrument_spec(instrument)

    async def get_rate_limits(self) -> RateLimits:
        return await self._adapter.get_rate_limits()

    async def fetch_positions(self) -> dict[Instrument, Position]:
        return await self._adapter.fetch_positions()

    async def fetch_balances(self) -> dict[str, Balance]:
        return await self._adapter.fetch_balances()

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        return await self._adapter.fetch_open_orders()

    async def fetch_fills(self) -> dict[str, list[FillRecord]]:
        return await self._adapter.fetch_fills()
