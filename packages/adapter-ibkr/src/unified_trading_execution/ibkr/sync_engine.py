"""SyncIBKREngine — single-object blocking API for Interactive Brokers trading.

Usage::

    from unified_trading_execution.ibkr import SyncIBKREngine, IBKRConfig

    engine = SyncIBKREngine(IBKRConfig(
        host="127.0.0.1",
        port=4002,
        client_id=1,
        account="DU123456",
    ))
    engine.connect()
    result = engine.place_order(order)
    engine.shutdown()

Adapter-specific methods (``fetch_positions``, ``fetch_balances``,
etc.) are not on the ABC — they are resolved at call time via
``__getattr__`` auto-proxy to the underlying ``IBKRAdapter``, dispatched
through the persistent background loop.  No extra delegation stubs needed.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal

from unified_trading_execution.events import EventBus
from unified_trading_execution.ibkr.adapter import IBKRAdapter
from unified_trading_execution.ibkr.config import IBKRConfig
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.types.instrument import Instrument


class SyncIBKREngine(SyncEngine):
    """All-in-one blocking engine for Interactive Brokers.

    Inherits every method from :class:`SyncEngine` (``place_order``,
    ``cancel_order``, ``reconcile``, history accessors, etc.).
    IBKR-specific methods (``fetch_positions``, ``fetch_balances``,
    etc.) are auto-proxied to the adapter via ``__getattr__`` — no
    explicit stubs needed.
    """

    def __init__(
        self,
        config: IBKRConfig | IBKRAdapter,
        *,
        state_store: StateStore | None = None,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
    ) -> None:
        adapter = config if isinstance(config, IBKRAdapter) else IBKRAdapter(config)
        super().__init__(
            adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
            risk_config=risk_config,
            halt_config=halt_config,
        )
