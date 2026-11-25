"""Core public surface re-exported by the shared namespace ``__init__``.

This module exists because every distribution ships its own copy of
``unified_trading_execution/__init__.py``.  Whichever copy pip installs last
wins, so each one reapplies this module's star import instead of repeating the
symbol list; otherwise installing an adapter after the core package would drop
the core top-level re-exports.
"""

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.engine import Engine
from unified_trading_execution.errors import (
    AccountChangedError,
    AccountHaltedError,
    ConnectionError,
    DuplicateOrderIdError,
    EngineShutdownError,
    InstrumentHaltedError,
    InsufficientBalanceError,
    InvalidSymbolError,
    MarketClosedError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
    ReconciliationError,
    TrailingNotSupportedError,
    TrailingStateError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import (
    AccountChangedEvent,
    AuditEvent,
    BalanceUpdateEvent,
    ConnectionStateEvent,
    Event,
    EventBus,
    FillEvent,
    HaltClearedEvent,
    HaltEnteredEvent,
    HaltEvent,
    OrderCancelledEvent,
    OrderModifiedEvent,
    OrderPlacedEvent,
    PositionUpdateEvent,
    ReconciliationCompleteEvent,
    ReconciliationEvent,
    ReconciliationMismatch,
    TickerEvent,
    TrailingStateEvent,
)
from unified_trading_execution.state import SQLiteStateStore, StateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types import (
    LIVE_ORDER_STATUSES,
    # Enums
    AssetClass,
    # Data types
    Balance,
    FillEntry,
    FillReason,
    FillRecord,
    HaltClearMode,
    HaltState,
    Instrument,
    InstrumentSpec,
    OptionRight,
    OrderModification,
    OrderRecord,
    OrderResult,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    TimeInForce,
    TpSlAttachment,
    UnifiedOrder,
)

__all__ = [  # noqa: RUF022
    # Enums
    "AssetClass",
    "FillEntry",
    "FillReason",
    "HaltClearMode",
    "HaltState",
    "LIVE_ORDER_STATUSES",
    "OptionRight",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "TimeInForce",
    # Data types
    "Balance",
    "FillRecord",
    "Instrument",
    "InstrumentSpec",
    "OrderModification",
    "OrderRecord",
    "OrderResult",
    "Position",
    "RateLimits",
    "TpSlAttachment",
    "UnifiedOrder",
    # Engine (async and sync)
    "Engine",
    "SyncEngine",
    # ABCs + default implementations
    "Adapter",
    "SQLiteStateStore",
    "StateStore",
    # Event bus
    "Event",
    "EventBus",
    "FillEvent",
    "PositionUpdateEvent",
    "BalanceUpdateEvent",
    "ConnectionStateEvent",
    "AccountChangedEvent",
    "OrderPlacedEvent",
    "OrderModifiedEvent",
    "OrderCancelledEvent",
    "TickerEvent",
    "TrailingStateEvent",
    "ReconciliationCompleteEvent",
    "ReconciliationEvent",
    "ReconciliationMismatch",
    "HaltEnteredEvent",
    "HaltClearedEvent",
    "HaltEvent",
    # Errors
    "AccountChangedError",
    "AccountHaltedError",
    "ConnectionError",
    "PlatformConnectionError",
    "DuplicateOrderIdError",
    "EngineShutdownError",
    "InstrumentHaltedError",
    "InsufficientBalanceError",
    "InvalidSymbolError",
    "MarketClosedError",
    "OrderNotFoundError",
    "PlatformError",
    "RateLimitError",
    "ReconciliationError",
    "TrailingNotSupportedError",
    "TrailingStateError",
    "UnsupportedOrderTypeError",
    # Audit
    "AuditEvent",
]
