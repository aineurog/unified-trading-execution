# Namespace package (pkgutil-style) — enables multiple independently-installable
# packages to contribute modules under a single unified_trading_execution namespace.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

# Core top-level re-exports — the public surface of unified_trading_execution.
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
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import (
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
)
from unified_trading_execution.state import SQLiteStateStore, StateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types import (
    # Enums
    AssetClass,
    FillEntry,
    FillReason,
    LIVE_ORDER_STATUSES,
    # Data types
    Balance,
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

__all__ = [
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
    "OrderPlacedEvent",
    "OrderModifiedEvent",
    "OrderCancelledEvent",
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
    "UnsupportedOrderTypeError",
    # Audit
    "AuditEvent",
]
