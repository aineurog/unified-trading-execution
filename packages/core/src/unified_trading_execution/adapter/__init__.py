"""Adapter ABC — the complete contract every platform adapter must implement.

No adapter method contains business logic, retry policy, or risk decisions.
Every method that can fail translates platform-native errors into the
common exception hierarchy before the error crosses the adapter boundary.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from unified_trading_execution.events import EventBus
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.types.enums import OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

if TYPE_CHECKING:
    from unified_trading_execution.state import StateStore


@dataclass(frozen=True, slots=True)
class RateLimits:
    """Platform's current rate-limit state — queried by the self-throttling validator."""

    requests_per_interval: int
    interval_seconds: float
    remaining: int
    reset_at: datetime


class Adapter(ABC):
    """Abstract base class for every platform adapter.

    Each adapter is constructed with its own configuration (credentials,
    testnet/live switch, etc.) and a reference to the EventBus. The adapter
    publishes translated events to this bus from its internal websocket handlers.

    The adapter does not hold a reference to the state mirror — it produces
    events that core's state mirror consumes.  ``attach_state_store`` is the one
    documented exception, scoped to adapter-owned intent persistence (e.g.
    leverage/margin-mode), never the mirror itself.
    """

    # ---- Identification ----

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Human-readable platform identifier (e.g. 'bybit', 'ctrader')."""
        ...

    @property
    @abstractmethod
    def account_id(self) -> str:
        """Unique account identifier on this platform."""
        ...

    async def resolve_account_id(self) -> str:
        """Return the canonical platform account identity.

        Defaults to :attr:`account_id`.  Adapters whose configured account
        label is not itself a unique platform identity override this to resolve
        the real identifier from the platform — e.g. Bybit returns its account
        ``uid`` from ``GET /v5/account/info``.  The engine calls this on connect
        to key the auto-derived state-store path, so two accounts of the same
        platform never collide on one file.  Implementations should degrade to
        ``self.account_id`` rather than raise when resolution is unavailable.
        """
        return self.account_id

    # ---- Connection lifecycle ----

    @abstractmethod
    async def connect(self) -> None:
        """Open persistent connections (REST session + WebSocket streams).

        Must publish ConnectionStateEvent(connected=True) on successful connect.
        """
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Close all connections gracefully.

        Must publish ConnectionStateEvent(connected=False) on disconnect.
        """
        ...

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Return True if connections are currently established."""
        ...

    # ---- Order operations ----

    @abstractmethod
    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order to the platform.

        Receives a UnifiedOrder that has already passed all risk checks.
        If the platform supports native TP/SL attachment, the adapter uses it.
        If not supported, raises UnsupportedOrderTypeError — never approximates.
        """
        ...

    @abstractmethod
    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Translate and submit an order modification to the platform.

        Core runs risk checks against the resulting order before calling this.
        Unsupported modification fields raise UnsupportedOrderTypeError.
        """
        ...

    @abstractmethod
    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order by client_order_id.

        Raises OrderNotFoundError if the platform does not know the order.
        """
        ...

    @abstractmethod
    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by client_order_id. Returns None if not found."""
        ...

    # ---- Instrument metadata ----

    @abstractmethod
    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch trading rules for a single instrument from the platform.

        Raises InvalidSymbolError if the instrument is not tradable.
        """
        ...

    # ---- Capability reporting ----

    @abstractmethod
    def supported_order_types(self) -> frozenset[OrderType]:
        """Return the set of order types this adapter supports.

        Must always include at minimum: {MARKET, LIMIT, STOP, STOP_LIMIT}.
        Core validates every order against this set before calling place_order.
        """
        ...

    # ---- Rate limits ----

    @abstractmethod
    async def get_rate_limits(self) -> RateLimits:
        """Return the platform's current rate-limit state.

        Queried by the self-throttling validator. Core may cache this briefly
        (TTL determined by interval_seconds) rather than calling on every dispatch.
        """
        ...

    # ---- Position TP/SL modification (optional) ----

    async def modify_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Modify TP/SL on an existing open position.

        Optional — raises ``NotImplementedError`` by default.  Platforms that
        support modifying TP/SL on positions (MT5 via ``TRADE_ACTION_SLTP``,
        IBKR via OCA orders, Bybit via ``set_trading_stop``) override this
        method.  At least one of *take_profit* or *stop_loss* must be provided.

        *position_id* is the platform-assigned position identifier (MT5 ticket,
        Bybit ``positionIdx``, IBKR ``conId``, ...).  It is scoped by
        *instrument* because some platforms reuse the same ``position_id``
        across instruments (e.g. Bybit's ``positionIdx`` is ``0`` for every
        one-way symbol).  Adapters whose ``position_id`` is globally unique may
        ignore *instrument*.  It is **not** the same as
        ``UnifiedOrder.position_id`` (a client-side passthrough); it is the
        platform's own position reference obtained from ``PositionUpdateEvent``
        or the state store.
        """
        raise NotImplementedError(
            f"{self.platform_name} does not support modifying TP/SL on open positions"
        )

    async def get_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
    ) -> tuple[TpSlAttachment | None, TpSlAttachment | None] | None:
        """Read the current TP/SL on an existing open position.

        Optional — raises ``NotImplementedError`` by default.  Returns
        ``(take_profit, stop_loss)`` where each element is ``None`` when that
        side has no stop set, or ``None`` when there is no open position at
        *position_id*.  ``position_id`` and *instrument* follow the same
        conventions as :meth:`modify_position_tpsl`.
        """
        raise NotImplementedError(
            f"{self.platform_name} does not support reading TP/SL on open positions"
        )

    # ---- Reconciliation data (optional — not required for basic operation) ----

    async def fetch_positions(self) -> list[Position]:
        """Fetch all open positions from the platform as a list of legs.

        One ``Position`` per terminal position leg: a hedged account yields
        multiple entries for the same instrument (each with its own
        ``position_id``), and a netted account yields a single entry.

        Optional: raises NotImplementedError by default. Adapters that
        implement this method enable full reconciliation.
        """
        raise NotImplementedError(f"{self.platform_name} does not support bulk position fetch")

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch all account balances from the platform, keyed by currency.

        Optional: raises NotImplementedError by default.
        """
        raise NotImplementedError(f"{self.platform_name} does not support bulk balance fetch")

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch all open orders from the platform, keyed by client_order_id.

        Optional: raises NotImplementedError by default.
        """
        raise NotImplementedError(f"{self.platform_name} does not support bulk order fetch")

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent fills from the platform, keyed by client_order_id.

        Optional: raises NotImplementedError by default.

        *since* is an optional lower bound (UTC) for the fill window, used by
        reconciliation to fetch only fills newer than the persisted "clean
        through" watermark.  When omitted, the adapter returns its own recent
        window.  Adapters that ignore *since* should still accept the keyword
        so core can call them uniformly.
        """
        raise NotImplementedError(f"{self.platform_name} does not support bulk fill fetch")

    # ---- Adapter-owned user intent (optional) ------------------------

    def attach_halt_machine(self, halt_machine: HaltStateMachine | None) -> None:
        """Optional: let core share its halt state machine with the adapter.

        Adapters that enforce adapter-owned user intent (e.g. Bybit leverage
        drift) override this to store the reference so they can enter
        instrument-scoped halts directly. Default no-op.
        """
        return None

    def attach_event_bus(self, event_bus: EventBus) -> None:
        """Let core share its EventBus with the adapter.

        The engine owns the single EventBus and hands it to the adapter via
        this hook so the adapter can publish translated events (fills,
        position/balance updates) without ever constructing a bus itself.
        Default no-op — adapters that publish events override this to store
        the reference.
        """
        return None

    def attach_state_store(self, state_store: StateStore) -> None:
        """Optional: provide the shared StateStore to the adapter.

        Adapters that persist adapter-owned intent (leverage/margin-mode)
        may use this hook to store a reference to the engine-managed StateStore.
        Default no-op so adapters that do not need the store are unaffected.
        """
        return None

    async def reconcile_user_intent(self) -> None:
        """Optional: reconcile adapter-owned user intent with the platform.

        Adapters that manage user intent (e.g. Bybit leverage / margin mode,
        Section 5.3) override this. Core calls it during a reconciliation pass
        so adapter-owned drift is corrected (reapplied / notified / halted)
        without core knowing adapter-specific types. Default no-op.
        """
        return None
