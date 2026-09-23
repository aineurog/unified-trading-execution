"""SyncHyperliquidEngine — single-object blocking API for Hyperliquid trading.

Thin ``SyncEngine`` subclass mirroring ``SyncBybitEngine``.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Literal

from unified_trading_execution.engine import (
    DEFAULT_FILL_SETTLE_LAG_SECONDS,
    DEFAULT_RECONCILE_INTERVAL_SECONDS,
)
from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid.adapter import HyperliquidAdapter
from unified_trading_execution.hyperliquid.config import HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types.instrument import Instrument


class SyncHyperliquidEngine(SyncEngine):
    """All-in-one blocking engine for Hyperliquid."""

    def __init__(
        self,
        config: HyperliquidConfig | HyperliquidAdapter,
        *,
        state_store: StateStore | None = None,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
        reconcile_interval_seconds: float | None = DEFAULT_RECONCILE_INTERVAL_SECONDS,
        fill_settle_lag_seconds: float = DEFAULT_FILL_SETTLE_LAG_SECONDS,
    ) -> None:
        adapter = config if isinstance(config, HyperliquidAdapter) else HyperliquidAdapter(config)
        super().__init__(
            adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
            risk_config=risk_config,
            halt_config=halt_config,
            reconcile_interval_seconds=reconcile_interval_seconds,
            fill_settle_lag_seconds=fill_settle_lag_seconds,
        )

    @property
    def _hl_adapter(self) -> HyperliquidAdapter:
        adapter = self.adapter
        assert isinstance(adapter, HyperliquidAdapter)
        return adapter

    def set_leverage(
        self,
        instrument: Instrument,
        *,
        leverage: int = 1,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset leverage via ``updateLeverage`` and persist intent."""
        raise NotImplementedError

    def get_leverage(self, instrument: Instrument) -> tuple[int, bool] | None:
        """Query per-asset ``(leverage, is_cross)`` from the venue."""
        raise NotImplementedError

    def remove_leverage(self, instrument: Instrument) -> None:
        """Drop stored per-asset leverage intent (venue untouched)."""
        raise NotImplementedError

    def top_up_isolated_margin(self, instrument: Instrument, *, amount_usdc: Decimal) -> None:
        """Top up isolated margin for an instrument."""
        raise NotImplementedError

    def set_margin_mode(
        self,
        instrument: Instrument,
        mode: MarginMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset margin mode via ``updateLeverage`` and persist intent."""
        raise NotImplementedError

    def get_margin_mode(self, instrument: Instrument) -> MarginMode | None:
        """Query the per-asset margin mode from the venue for an instrument."""
        raise NotImplementedError

    def remove_margin_mode(self, instrument: Instrument) -> None:
        """Drop stored per-asset margin-mode intent (venue untouched)."""
        raise NotImplementedError

    def reconcile_user_intent(self) -> None:
        """Reconcile stored per-asset leverage/mode intent with the venue."""
        raise NotImplementedError
