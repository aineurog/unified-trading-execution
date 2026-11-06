"""Async Binance engine scaffold."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from unified_trading_execution.adapter import Adapter
from unified_trading_execution.binance.adapter import BinanceAdapter
from unified_trading_execution.binance.config import BinanceConfig
from unified_trading_execution.engine import (
    DEFAULT_FILL_SETTLE_LAG_SECONDS,
    DEFAULT_RECONCILE_INTERVAL_SECONDS,
    Engine,
)
from unified_trading_execution.events import EventBus
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.trailing import TrailingConfig
from unified_trading_execution.types.instrument import Instrument


class BinanceEngine(Engine):
    """All-in-one async engine for Binance.

    The inherited core API is available, but exchange operations raise
    ``NotImplementedError`` until their dedicated implementation branches.
    """

    def __init__(
        self,
        config: BinanceConfig | BinanceAdapter,
        *,
        state_store: StateStore | None = None,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
        trailing_config: TrailingConfig | None = None,
        reconcile_interval_seconds: float | None = DEFAULT_RECONCILE_INTERVAL_SECONDS,
        fill_settle_lag_seconds: float = DEFAULT_FILL_SETTLE_LAG_SECONDS,
    ) -> None:
        adapter: Adapter = config if isinstance(config, BinanceAdapter) else BinanceAdapter(config)
        super().__init__(
            adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
            risk_config=risk_config,
            halt_config=halt_config,
            trailing_config=trailing_config,
            reconcile_interval_seconds=reconcile_interval_seconds,
            fill_settle_lag_seconds=fill_settle_lag_seconds,
        )

    async def connect(self) -> None:
        """Connect the Binance adapter through the core engine lifecycle."""
        await super().connect()
