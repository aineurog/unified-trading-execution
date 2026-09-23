"""HyperliquidAdapter — concrete Adapter ABC implementation for Hyperliquid.

Covers Hyperliquid spot and perpetual markets.  One ``Exchange`` instance
per process (the SDK nonce state is instance-scoped); every blocking SDK
call goes through ``asyncio.to_thread`` with the configured timeout and is
never awaited on the loop thread.  Identity is the wallet address itself —
no separate uid-resolve step.  One-way positions only — no hedge routing.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.events import EventBus
from unified_trading_execution.hyperliquid.config import HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.types.enums import OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position


class HyperliquidAdapter(Adapter):
    """Concrete Adapter ABC implementation for Hyperliquid."""

    def __init__(
        self,
        config: HyperliquidConfig,
        *,
        event_bus: EventBus | None = None,
        state_store: StateStore | None = None,
    ) -> None:
        self._config = config
        self._event_bus = event_bus
        self._state_store = state_store
        self._halt_machine: HaltStateMachine | None = None
        self._connected = False

    # ---- Identification ----

    @property
    def platform_name(self) -> str:
        return self._config.platform_name

    @property
    def account_id(self) -> str:
        """The wallet address is the canonical account identity."""
        return self._config.wallet_address

    async def resolve_account_id(self) -> str:
        """Return the canonical identity — the wallet address itself."""
        return self._config.wallet_address

    # ---- Adapter-owned wiring ----

    def attach_halt_machine(self, halt_machine: HaltStateMachine | None) -> None:
        """Store core's shared halt state machine so drift can enter halts."""
        self._halt_machine = halt_machine

    def attach_event_bus(self, event_bus: EventBus) -> None:
        """Store the engine's shared event bus so WS handlers can publish."""
        self._event_bus = event_bus

    def attach_state_store(self, state_store: StateStore) -> None:
        """Receive the engine-managed StateStore for per-asset intent."""
        self._state_store = state_store

    # ---- Connection lifecycle ----

    async def connect(self) -> None:
        """Open REST/WS connections and verify signing identity.

        Asserts the unified account mode and the key's ``info.userRole``
        (the ``approveAgent`` one-time setup must have been completed with
        the main key).  Publishes ``ConnectionStateEvent(connected=True)``
        on success.
        """
        raise NotImplementedError

    async def disconnect(self) -> None:
        """Close all connections gracefully.

        Publishes ``ConnectionStateEvent(connected=False)``; cancels
        in-flight posts; leaves no dangling nonce gaps.
        """
        raise NotImplementedError

    @property
    def is_connected(self) -> bool:
        """Return True if connections are currently established."""
        raise NotImplementedError

    # ---- Order operations ----

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order.

        Receives a ``UnifiedOrder`` that has already passed all risk checks.
        ``cloid``-addressed end-to-end for idempotent retry.
        """
        raise NotImplementedError

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Translate and submit an order modification, preferring cloid."""
        raise NotImplementedError

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order via ``cancelByCloid``.

        Raises ``OrderNotFoundError`` if the venue reports the order was
        never placed.
        """
        raise NotImplementedError

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by cloid via the cloid index and ``orderStatus``.

        Returns None if not found.
        """
        raise NotImplementedError

    # ---- Instrument metadata ----

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch trading rules for an instrument from ``meta``/``spotMeta``.

        Raises ``InvalidSymbolError`` if the instrument is not tradable.
        """
        raise NotImplementedError

    # ---- Capability reporting ----

    def supported_order_types(self) -> frozenset[OrderType]:
        """Return the supported order types — all four guaranteed types."""
        raise NotImplementedError

    # ---- Rate limits ----

    async def get_rate_limits(self) -> RateLimits:
        """Return the live weight-budget state, not constants."""
        raise NotImplementedError

    # ---- Market data ----

    async def fetch_ticker(self, instrument: Instrument) -> Ticker | None:
        """Snapshot bid/ask/mid via ``allMids`` + ``l2Book``.

        Weight-2 info calls, ~1s cache.  Returns None when the book is empty.
        """
        raise NotImplementedError

    # ---- Position TP/SL modification ----

    async def modify_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Modify TP/SL on an open position via the ``positionTpsl`` grouping."""
        raise NotImplementedError

    async def get_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
    ) -> tuple[TpSlAttachment | None, TpSlAttachment | None] | None:
        """Read the current TP/SL on an open position."""
        raise NotImplementedError

    # ---- Reconciliation data ----

    async def fetch_positions(self) -> list[Position]:
        """Fetch open legs from ``clearinghouseState``."""
        raise NotImplementedError

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch per-currency balances from ``spotClearinghouseState``."""
        raise NotImplementedError

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch open orders (``openOrders`` + ``frontendOpenOrders``) keyed by cloid."""
        raise NotImplementedError

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent fills (``userFills`` + ``userFillsByTime`` merged and deduped)."""
        raise NotImplementedError

    # ---- Leverage + margin intent ----

    async def set_leverage(
        self,
        instrument: Instrument,
        *,
        leverage: int = 1,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset leverage via ``updateLeverage`` and persist intent.

        Owns the leverage number only — the margin mode comes from the
        separate margin-mode intent (stored intent, else live venue value,
        else the configured default), so each field has exactly one writer.
        Max is checked against both ``universe.maxLeverage`` and the
        ``marginTables`` tiers — block with ``InvalidOrderError``, never
        silently clamp.
        """
        raise NotImplementedError

    async def get_leverage(self, instrument: Instrument) -> tuple[int, bool] | None:
        """Query per-asset ``(leverage, is_cross)`` from the venue."""
        raise NotImplementedError

    async def remove_leverage(self, instrument: Instrument) -> None:
        """Drop stored per-asset leverage intent (venue untouched)."""
        raise NotImplementedError

    async def top_up_isolated_margin(self, instrument: Instrument, *, amount_usdc: Decimal) -> None:
        """Top up isolated margin via ``updateIsolatedMargin`` semantics.

        ``isBuy`` is passed as ``true`` — a documented venue no-op until
        Hyperliquid hedge mode exists.
        """
        raise NotImplementedError

    async def set_margin_mode(
        self,
        instrument: Instrument,
        mode: MarginMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset margin mode via ``updateLeverage`` and persist intent.

        Owns the mode only — leverage is preserved (stored leverage intent,
        else live venue value, else the configured default).  ``mode`` is
        the ``MarginMode`` enum or the raw strings ``"cross"`` /
        ``"isolated"``.  Each intent carries its own drift policy, so mode
        drift and leverage drift are managed independently.
        """
        raise NotImplementedError

    async def get_margin_mode(self, instrument: Instrument) -> MarginMode | None:
        """Query the per-asset margin mode from the venue for an instrument."""
        raise NotImplementedError

    async def remove_margin_mode(self, instrument: Instrument) -> None:
        """Drop stored per-asset margin-mode intent (venue untouched)."""
        raise NotImplementedError

    async def reconcile_user_intent(self) -> None:
        """Reconcile stored per-asset leverage/mode intent with the venue.

        Re-applies drifted leverage/mode per the stored on-drift policy.
        No position-mode reconciliation — one-way is asserted, not managed.
        """
        raise NotImplementedError
