"""Adapter ABC — the complete contract every platform adapter must implement.

No adapter method contains business logic, retry policy, or risk decisions.
Every method that can fail translates platform-native errors into the
common exception hierarchy before the error crosses the adapter boundary.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from unified_trading_execution.events import EventBus
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.types.enums import OrderType
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position


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

    The adapter never holds a reference to the StateStore — it produces events;
    core's state mirror consumes them.
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

    # ---- Reconciliation data (optional — not required for basic operation) ----

    async def fetch_positions(self) -> dict[Instrument, Position]:
        """Fetch all open positions from the platform, keyed by Instrument.

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

    async def fetch_fills(self) -> dict[str, list[FillRecord]]:
        """Fetch recent fills from the platform, keyed by client_order_id.

        Optional: raises NotImplementedError by default.
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

    async def reconcile_user_intent(self) -> None:
        """Optional: reconcile adapter-owned user intent with the platform.

        Adapters that manage user intent (e.g. Bybit leverage / margin mode,
        Section 5.3) override this. Core calls it during a reconciliation pass
        so adapter-owned drift is corrected (reapplied / notified / halted)
        without core knowing adapter-specific types. Default no-op.
        """
        return None
