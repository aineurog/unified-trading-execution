"""HyperliquidEngine — single-object async API for Hyperliquid trading.

Thin ``Engine`` subclass mirroring ``BybitEngine``: position-TP/SL
convenience, leverage/margin intents and snapshot proxies, with no business
logic outside the adapter.  One structural difference from Bybit: the
adapter leaves push streams stopped after ``connect()``, so this engine
starts them — ``await engine.connect()`` brings up REST *and* streams,
matching the single-call contract.  Shutdown needs nothing extra: core
``ashutdown()`` calls adapter ``disconnect()``, which stops streams first.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import Literal

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.engine import (
    DEFAULT_FILL_SETTLE_LAG_SECONDS,
    DEFAULT_RECONCILE_INTERVAL_SECONDS,
    Engine,
)
from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid.adapter import HyperliquidAdapter
from unified_trading_execution.hyperliquid.config import HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.order import FillRecord, OrderRecord, TpSlAttachment
from unified_trading_execution.types.position import Balance, Position


class HyperliquidEngine(Engine):
    """All-in-one async engine for Hyperliquid.

    Inherits every method from :class:`Engine` (``place_order``,
    ``cancel_order``, ``reconcile``, history accessors, etc.) and adds
    Hyperliquid-specific methods directly — one object, one import, zero wiring.
    """

    _adapter: HyperliquidAdapter

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

    async def connect(self) -> None:
        """Connect REST via core, then open the push channel.

        Everything core does (store, mirror seed, halts, rate limits,
        reconcile loop) runs first; streams attach last so handlers observe
        a fully wired engine from their first message.
        """
        await super().connect()
        await self._adapter.start_streams()

    # ── leverage intent ───────────────────────────────────────────────

    async def set_leverage(
        self,
        instrument: Instrument,
        *,
        leverage: int = 1,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        strict_check: bool = True,
        block_on_open_position: bool = True,
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset leverage via ``updateLeverage`` and persist intent."""
        await self._adapter.set_leverage(
            instrument,
            leverage=leverage,
            on_drift=on_drift,
            strict_check=strict_check,
            block_on_open_position=block_on_open_position,
            auto_apply_on_connect=auto_apply_on_connect,
        )

    async def get_leverage(self, instrument: Instrument) -> tuple[int, bool] | None:
        """Query per-asset ``(leverage, is_cross)`` from the venue."""
        return await self._adapter.get_leverage(instrument)

    async def remove_leverage(self, instrument: Instrument) -> None:
        """Drop stored per-asset leverage intent (venue untouched)."""
        await self._adapter.remove_leverage(instrument)

    async def top_up_isolated_margin(self, instrument: Instrument, *, amount_usdc: Decimal) -> None:
        """Top up isolated margin for an instrument."""
        await self._adapter.top_up_isolated_margin(instrument, amount_usdc=amount_usdc)

    # ── margin-mode intent ────────────────────────────────────────────

    async def set_margin_mode(
        self,
        instrument: Instrument,
        mode: MarginMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        block_on_open_position: bool = True,
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset margin mode via ``updateLeverage`` and persist intent."""
        await self._adapter.set_margin_mode(
            instrument,
            mode,
            on_drift=on_drift,
            block_on_open_position=block_on_open_position,
            auto_apply_on_connect=auto_apply_on_connect,
        )

    async def get_margin_mode(self, instrument: Instrument) -> MarginMode | None:
        """Query the per-asset margin mode from the venue for an instrument."""
        return await self._adapter.get_margin_mode(instrument)

    async def remove_margin_mode(self, instrument: Instrument) -> None:
        """Drop stored per-asset margin-mode intent (venue untouched)."""
        await self._adapter.remove_margin_mode(instrument)

    async def reconcile_user_intent(self) -> None:
        """Reconcile stored per-asset leverage/mode intent with the venue."""
        await self._adapter.reconcile_user_intent()

    # ── snapshots / reads ─────────────────────────────────────────────

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch (or return a cached) ``InstrumentSpec`` for ``instrument``."""
        return await self._adapter.fetch_instrument_spec(instrument)

    async def fetch_ticker(self, instrument: Instrument) -> Ticker | None:
        """Fetch the latest price snapshot for *instrument* as a :class:`Ticker`."""
        return await self._adapter.fetch_ticker(instrument)

    async def get_rate_limits(self) -> RateLimits:
        """Return the live IP weight-budget state."""
        return await self._adapter.get_rate_limits()

    async def fetch_positions(self) -> list[Position]:
        """Fetch open legs from ``clearinghouseState`` (spot has no legs)."""
        return await self._adapter.fetch_positions()

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch per-currency balances from ``spotClearinghouseState``."""
        return await self._adapter.fetch_balances()

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch every open order, keyed by client order id."""
        return await self._adapter.fetch_open_orders()

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent fills, grouped by client order id."""
        return await self._adapter.fetch_fills(since=since)

    # ── position TP/SL ────────────────────────────────────────────────

    async def modify_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Modify TP/SL on an open position (``position_id`` = ``"<coin>:oneWay"``
        from ``fetch_positions``)."""
        await self._adapter.modify_position_tpsl(
            instrument,
            position_id,
            take_profit=take_profit,
            stop_loss=stop_loss,
        )

    async def get_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
    ) -> tuple[TpSlAttachment | None, TpSlAttachment | None] | None:
        """Read the current TP/SL on an open position as ``(take_profit, stop_loss)``."""
        return await self._adapter.get_position_tpsl(instrument, position_id)
