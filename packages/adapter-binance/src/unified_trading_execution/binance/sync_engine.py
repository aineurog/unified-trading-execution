"""Synchronous Binance engine scaffold."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from unified_trading_execution.binance.adapter import BinanceAdapter
from unified_trading_execution.binance.config import BinanceConfig
from unified_trading_execution.engine import (
    DEFAULT_FILL_SETTLE_LAG_SECONDS,
    DEFAULT_RECONCILE_INTERVAL_SECONDS,
)
from unified_trading_execution.events import EventBus
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.trailing import TrailingConfig
from unified_trading_execution.types.instrument import Instrument


class SyncBinanceEngine(SyncEngine):
    """Blocking facade for Binance using the core persistent-loop engine."""

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
        adapter = config if isinstance(config, BinanceAdapter) else BinanceAdapter(config)
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

    def connect(self) -> None:
        """Fail before starting the sync loop for this scaffold."""
        raise NotImplementedError("Binance sync engine scaffold: connect is not implemented")
