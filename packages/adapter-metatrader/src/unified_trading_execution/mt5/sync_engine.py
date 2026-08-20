"""SyncMT5Engine — single-object blocking API for MetaTrader 5 trading.

Usage::

    from unified_trading_execution.mt5 import SyncMT5Engine, MT5Config

    engine = SyncMT5Engine(MT5Config(
        login=12345678,
        password="hunter2",
        server="ICMarkets-Demo",
    ))
    engine.connect()
    result = engine.place_order(order)
    engine.modify_position_tpsl("12345", take_profit=...)
    engine.shutdown()

Adapter-specific methods (``modify_position_tpsl``, ``fetch_positions``,
etc.) are not on the ABC — they are resolved at call time via
``__getattr__`` auto-proxy to the underlying ``MT5Adapter``, dispatched
through the persistent background loop.  No extra delegation stubs needed.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from unified_trading_execution.events import EventBus
from unified_trading_execution.mt5.adapter import MT5Adapter
from unified_trading_execution.mt5.config import MT5Config
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types.instrument import Instrument


class SyncMT5Engine(SyncEngine):
    """All-in-one blocking engine for MetaTrader 5.

    Inherits every method from :class:`SyncEngine` (``place_order``,
    ``cancel_order``, ``reconcile``, history accessors, etc.).
    MT5-specific methods (``modify_position_tpsl``, ``fetch_positions``,
    ``fetch_balances``, etc.) are auto-proxied to the adapter via
    ``__getattr__`` — no explicit stubs needed.
    """

    def __init__(
        self,
        config: MT5Config | MT5Adapter,
        *,
        state_store: StateStore | None = None,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
    ) -> None:
        adapter = config if isinstance(config, MT5Adapter) else MT5Adapter(config)
        super().__init__(
            adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
            risk_config=risk_config,
            halt_config=halt_config,
        )
