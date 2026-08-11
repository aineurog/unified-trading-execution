"""SyncBybitAdapter - blocking facade over BybitAdapter for use with SyncEngine.

Thin wrapper: the BybitAdapter stays the single async implementation; every
sync method here just dispatches the corresponding coroutine onto the same
persistent background loop the SyncEngine owns (via ``SyncEngine.run``).  One
loop, one thread, everything serialized - no ``asyncio.run()``, no coroutine
leaked into user code.

Usage::

    from unified_trading_execution import SyncEngine
    from unified_trading_execution.bybit import BybitAdapter, BybitConfig, SyncBybitAdapter

    engine = SyncEngine(adapter=BybitAdapter(BybitConfig(api_key=..., api_secret=...)))
    adapter = SyncBybitAdapter(engine)
    engine.connect()

    adapter.set_leverage(btc_perp, buy_leverage=10)
    positions = adapter.fetch_positions()
    mode = adapter.get_margin_mode()
"""

from __future__ import annotations

from typing import Any

import unified_trading_execution as ute
from unified_trading_execution.bybit.adapter import BybitAdapter
from unified_trading_execution.bybit.enums import MarginMode, PositionMode


class SyncBybitAdapter:
    """Blocking wrapper over :class:`BybitAdapter`, bound to a :class:`SyncEngine`.

    Every public method mirrors an async ``BybitAdapter`` method and blocks the
    calling thread until the coroutine completes on the engine's background
    loop.  Order lifecycle (``place_order`` / ``modify_order`` / ``cancel_order``)
    stays on ``SyncEngine`` - that path runs through the risk + state-mirror
    pipeline; this wrapper covers the adapter-specific surface (leverage,
    position mode, margin mode, snapshots, open orders, fills, rate limits).
    """

    def __init__(self, engine: ute.SyncEngine) -> None:
        self._engine = engine
        adapter = engine.adapter
        if not isinstance(adapter, BybitAdapter):
            raise TypeError(
                f"SyncBybitAdapter requires a BybitAdapter, got {type(adapter).__name__}"
            )
        self._adapter: BybitAdapter = adapter

    # ---- passthrough attributes ----

    @property
    def adapter(self) -> BybitAdapter:
        """The underlying async adapter (read-only access to the async surface)."""
        return self._adapter

    @property
    def platform_name(self) -> str:
        return self._adapter.platform_name

    @property
    def account_id(self) -> str:
        return self._adapter.account_id

    @property
    def is_connected(self) -> bool:
        return self._adapter.is_connected

    # ---- leverage intent ----

    def set_leverage(
        self,
        instrument: ute.Instrument,
        *,
        buy_leverage: int = 50,
        sell_leverage: int | None = None,
        on_drift: str = "reapply",
        strict_check: bool = True,
        block_on_open_position: bool = True,
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set leverage for *instrument* on the platform and persist the intent."""
        self._run(
            self._adapter.set_leverage(
                instrument,
                buy_leverage=buy_leverage,
                sell_leverage=sell_leverage,
                on_drift=on_drift,
                strict_check=strict_check,
                block_on_open_position=block_on_open_position,
                auto_apply_on_connect=auto_apply_on_connect,
            )
        )

    def get_leverage(self, instrument: ute.Instrument) -> tuple[int, int] | None:
        """Return the platform's current (buy, sell) leverage for *instrument*."""
        return self._run(self._adapter.get_leverage(instrument))

    def remove_leverage(self, instrument: ute.Instrument) -> None:
        """Delete the stored leverage intent for *instrument*."""
        self._run(self._adapter.remove_leverage(instrument))

    # ---- position mode intent ----

    def set_position_mode(
        self,
        instrument: ute.Instrument,
        mode: str | PositionMode,
        *,
        on_drift: str = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set position mode for *instrument* on the platform and persist intent."""
        self._run(
            self._adapter.set_position_mode(
                instrument,
                mode,
                on_drift=on_drift,
                auto_apply_on_connect=auto_apply_on_connect,
            )
        )

    def get_position_mode(self, instrument: ute.Instrument) -> PositionMode | None:
        """Return the platform's current position mode for *instrument*."""
        return self._run(self._adapter.get_position_mode(instrument))

    def remove_position_mode(self, instrument: ute.Instrument) -> None:
        """Delete the stored position-mode intent for *instrument*."""
        self._run(self._adapter.remove_position_mode(instrument))

    def set_position_mode_for_coin(
        self,
        coin: str,
        category: str,
        mode: str | PositionMode,
    ) -> None:
        """Batch-switch position mode for all symbols of *coin* with no open positions."""
        self._run(
            self._adapter.set_position_mode_for_coin(coin, category, mode)
        )

    # ---- margin mode ----

    def set_margin_mode(self, mode: MarginMode) -> None:
        """Set the account-wide margin mode on the platform."""
        self._run(self._adapter.set_margin_mode(mode))

    def get_margin_mode(self) -> MarginMode | None:
        """Return the account's current margin mode."""
        return self._run(self._adapter.get_margin_mode())

    # ---- snapshots / reads ----

    def fetch_instrument_spec(self, instrument: ute.Instrument) -> ute.InstrumentSpec:
        """Fetch and cache trading rules for *instrument*."""
        return self._run(self._adapter.fetch_instrument_spec(instrument))

    def get_rate_limits(self) -> ute.RateLimits:
        """Return the adapter's current rate-limit budget."""
        return self._run(self._adapter.get_rate_limits())

    def fetch_positions(self) -> dict[ute.Instrument, ute.Position]:
        """Fetch all Bybit positions across every applicable category."""
        return self._run(self._adapter.fetch_positions())

    def fetch_balances(self) -> dict[str, ute.Balance]:
        """Fetch the account's per-coin balance, keyed by currency."""
        return self._run(self._adapter.fetch_balances())

    def fetch_open_orders(self) -> dict[str, ute.OrderRecord]:
        """Fetch every open order, keyed by client order id."""
        return self._run(self._adapter.fetch_open_orders())

    def fetch_fills(self) -> dict[str, list[ute.FillRecord]]:
        """Fetch recent fills grouped by client order id."""
        return self._run(self._adapter.fetch_fills())

    # ---- lifecycle mirror (delegates to the engine loop) ----

    def connect(self) -> None:
        """Connect the underlying adapter (idempotent with engine.connect())."""
        self._run(self._adapter.connect())

    def disconnect(self) -> None:
        """Disconnect the underlying adapter gracefully."""
        self._run(self._adapter.disconnect())

    def reconcile_user_intent(self) -> None:
        """Reconcile stored leverage / position-mode intent against platform."""
        self._run(self._adapter.reconcile_user_intent())

    # ---- internal ----

    def _run(self, coro: Any) -> Any:
        return self._engine.run(coro)
