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

from unified_trading_execution.adapter import CandleTimeframe
from unified_trading_execution.events import EventBus
from unified_trading_execution.ibkr.adapter import IBKRAdapter
from unified_trading_execution.ibkr.config import IBKRConfig
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import HaltConfig, StateStore
from unified_trading_execution.sync import SyncEngine
from unified_trading_execution.trailing import TrailingConfig
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.market_data import Candle
from unified_trading_execution.types.order import TpSlAttachment


class SyncIBKREngine(SyncEngine):
    """All-in-one blocking engine for Interactive Brokers.

    Inherits every method from :class:`SyncEngine` (``place_order``,
    ``cancel_order``, ``reconcile``, history accessors, etc.).
    IBKR-specific methods (``fetch_positions``, ``fetch_balances``,
    etc.) are auto-proxied to the adapter via ``__getattr__`` — no
    explicit stubs needed.
    """

    @property
    def _ibkr_adapter(self) -> IBKRAdapter:
        adapter = self.adapter
        assert isinstance(adapter, IBKRAdapter)
        return adapter

    def __init__(
        self,
        config: IBKRConfig | IBKRAdapter,
        *,
        state_store: StateStore | None = None,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
        trailing_config: TrailingConfig | None = None,
    ) -> None:
        adapter = config if isinstance(config, IBKRAdapter) else IBKRAdapter(config)
        super().__init__(
            adapter,
            state_store=state_store,
            get_reference_price=get_reference_price,
            event_bus=event_bus,
            risk_config=risk_config,
            halt_config=halt_config,
            trailing_config=trailing_config,
        )

    # ── Position TP/SL ───────────────────────────────────────────────

    def modify_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Modify TP/SL on an open IBKR position (``position_id`` = ``str(conId)``)."""
        self._run(
            self._ibkr_adapter.modify_position_tpsl(
                instrument,
                position_id,
                take_profit=take_profit,
                stop_loss=stop_loss,
            )
        )

    def get_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
    ) -> tuple[TpSlAttachment | None, TpSlAttachment | None] | None:
        """Read the current TP/SL on an open IBKR position as ``(take_profit, stop_loss)``."""
        return self._run(self._ibkr_adapter.get_position_tpsl(instrument, position_id))

    def clear_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: bool = False,
        stop_loss: bool = False,
    ) -> None:
        """Clear the named protection sides on an open IBKR position leg."""
        self._run(
            self._ibkr_adapter.clear_position_tpsl(
                instrument,
                position_id,
                take_profit=take_profit,
                stop_loss=stop_loss,
            )
        )

    # ── Market data ─────────────────────────────────────────────────

    def fetch_candles(
        self,
        instrument: Instrument,
        timeframe: CandleTimeframe,
        *,
        limit: int,
    ) -> list[Candle]:
        """Fetch closed OHLC candles for ATR."""
        return self._run(self._ibkr_adapter.fetch_candles(instrument, timeframe, limit=limit))
