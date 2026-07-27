# Namespace package (pkgutil-style) — enables multiple independently-installable
# packages to contribute modules under a single unified_trading_execution namespace.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

# Core top-level re-exports — the public surface of unified_trading_execution.
from unified_trading_execution.types import (
    # Enums
    AssetClass,
    HaltClearMode,
    HaltState,
    OptionRight,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    # Data types
    Balance,
    FillRecord,
    Instrument,
    InstrumentSpec,
    OrderModification,
    OrderRecord,
    OrderResult,
    Position,
    RateLimits,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.errors import (
    AccountHaltedError,
    ConnectionError,
    DuplicateOrderIdError,
    EngineShutdownError,
    InstrumentHaltedError,
    InsufficientBalanceError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformError,
    RateLimitError,
    UnsupportedOrderTypeError,
)
from unified_trading_execution.events import (
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
from unified_trading_execution.adapter import Adapter
from unified_trading_execution.engine import Engine
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.state import SQLiteStateStore, StateStore
from unified_trading_execution.logging import AuditEvent

__all__ = [
    # Enums
    "AssetClass",
    "HaltClearMode",
    "HaltState",
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
    "AccountHaltedError",
    "ConnectionError",
    "DuplicateOrderIdError",
    "EngineShutdownError",
    "InstrumentHaltedError",
    "InsufficientBalanceError",
    "InvalidSymbolError",
    "OrderNotFoundError",
    "PlatformError",
    "RateLimitError",
    "UnsupportedOrderTypeError",
    # Audit
    "AuditEvent",
]
