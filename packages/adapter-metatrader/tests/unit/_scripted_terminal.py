"""Scripted MetaTrader5 terminal for the adapter trailing tests.

The real ``MetaTrader5`` package is Windows-only and talks to a live terminal.
These tests drive the adapter through an in-memory terminal that models position
legs, quotes, candles, and ``TRADE_ACTION_SLTP`` writes so the trailing surface
can be exercised end to end without credentials.

The module is named with a leading underscore so pytest does not collect it.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

DEFAULT_SYMBOL = "EURUSD.m"
TICK_SIZE = Decimal("0.0001")
TRADE_ACTION_SLTP = 6
TRADE_RETCODE_DONE = 10009
TRADE_RETCODE_REJECT = 10006


@dataclass
class ScriptedLeg:
    """One terminal position leg, mirroring the fields the adapter reads."""

    ticket: int
    side: int = 0  # 0 = BUY, 1 = SELL
    volume: float = 0.1
    price_open: float = 1.1000
    opened_at: int = 0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    symbol: str = DEFAULT_SYMBOL

    def snapshot(self) -> SimpleNamespace:
        return SimpleNamespace(
            ticket=self.ticket,
            time=self.opened_at,
            time_update=self.opened_at,
            type=self.side,
            symbol=self.symbol,
            volume=self.volume,
            price_open=self.price_open,
            sl=self.stop_loss,
            tp=self.take_profit,
        )


def _order_result(
    request: dict[str, Any],
    *,
    retcode: int = TRADE_RETCODE_DONE,
    comment: str = "",
) -> SimpleNamespace:
    return SimpleNamespace(
        retcode=retcode,
        deal=0,
        order=0,
        volume=0.0,
        price=0.0,
        bid=0.0,
        ask=0.0,
        comment=comment,
        request=request,
    )


class ScriptedTerminal:
    """A stateful stand-in for the process-global ``MetaTrader5`` module."""

    def __init__(
        self,
        *,
        legs: list[ScriptedLeg] | None = None,
        bid: float = 1.1000,
        ask: float = 1.1000,
        last: float = 1.1000,
        offset_seconds: int = 0,
    ) -> None:
        self.legs: dict[int, ScriptedLeg] = {leg.ticket: leg for leg in legs or []}
        self.bid = bid
        self.ask = ask
        self.last = last
        self.offset_seconds = offset_seconds
        self.trade_stops_level = 100
        self.spread_points = 20
        self.sends: list[dict[str, Any]] = []
        self.candle_fetches: list[tuple[str, int, int, int]] = []
        self.ticker_available = True
        self.reject_next_send = False
        self._atr_bars = [
            (1.0990, 1.1000, 1.0980, 1.0990),
            (1.0990, 1.1010, 1.0990, 1.1000),
            (1.1000, 1.1020, 1.1000, 1.1010),
            (1.1010, 1.1030, 1.1010, 1.1020),
        ]

    # -- Installation -----------------------------------------------------

    def install(self, mt5: Any) -> None:
        """Route every adapter terminal call through this scripted instance."""
        mt5.initialize.return_value = True
        mt5.shutdown.return_value = True
        mt5.account_info.side_effect = self.account_info
        mt5.positions_get.side_effect = self.positions_get
        mt5.orders_get.side_effect = lambda *args, **kwargs: ()
        mt5.history_deals_get.side_effect = lambda *args, **kwargs: ()
        mt5.order_send.side_effect = self.order_send
        mt5.symbol_info.side_effect = self.symbol_info
        mt5.symbol_info_tick.side_effect = self.symbol_info_tick
        mt5.symbol_select.side_effect = lambda *args, **kwargs: True
        mt5.symbols_get.side_effect = lambda *args, **kwargs: ()
        mt5.copy_rates_from_pos.side_effect = self.copy_rates_from_pos
        mt5.last_error.return_value = (1, "")
        mt5.TRADE_ACTION_SLTP = TRADE_ACTION_SLTP
        mt5.TRADE_ACTION_DEAL = 1
        mt5.TRADE_ACTION_MODIFY = 2
        mt5.TRADE_ACTION_REMOVE = 3
        mt5.TRADE_ACTION_PENDING = 5
        mt5.TRADE_RETCODE_DONE = TRADE_RETCODE_DONE
        mt5.TRADE_RETCODE_PLACED = 10008
        mt5.TRADE_RETCODE_DONE_PARTIAL = 10010
        mt5.TRADE_RETCODE_NO_CHANGES = 10025
        mt5.TRADE_RETCODE_REJECT = TRADE_RETCODE_REJECT
        mt5.RES_S_OK = 1

    # -- Terminal reads ---------------------------------------------------

    def server_epoch(self) -> int:
        return int(time.time()) + self.offset_seconds

    def account_info(self) -> SimpleNamespace:
        return SimpleNamespace(
            login=12345678,
            currency="USD",
            balance=1000.0,
            equity=1000.0,
            margin=0.0,
            margin_free=1000.0,
            leverage=100,
        )

    def positions_get(self, ticket: int | None = None) -> tuple[SimpleNamespace, ...]:
        if ticket is not None:
            leg = self.legs.get(int(ticket))
            return (leg.snapshot(),) if leg is not None else ()
        return tuple(leg.snapshot() for leg in self.legs.values())

    def symbol_info(self, symbol: str) -> SimpleNamespace | None:
        if symbol != DEFAULT_SYMBOL:
            return None
        return SimpleNamespace(
            name=symbol,
            path="Forex\\EURUSD",
            currency_base="EUR",
            currency_profit="USD",
            trade_calc_mode=0,
            trade_mode=4,
            volume_step=0.01,
            volume_min=0.01,
            volume_max=100.0,
            trade_tick_size=float(TICK_SIZE),
            point=float(TICK_SIZE),
            digits=5,
            trade_stops_level=self.trade_stops_level,
            spread=self.spread_points,
            filling_mode=0b100,
        )

    def symbol_info_tick(self, symbol: str) -> SimpleNamespace | None:
        if symbol != DEFAULT_SYMBOL or not self.ticker_available:
            return None
        epoch = self.server_epoch()
        return SimpleNamespace(
            bid=self.bid,
            ask=self.ask,
            last=self.last,
            time=epoch,
            time_msc=epoch * 1000,
        )

    def copy_rates_from_pos(
        self,
        symbol: str,
        timeframe: int,
        start: int,
        count: int,
    ) -> list[dict[str, Any]] | None:
        self.candle_fetches.append((symbol, timeframe, start, count))
        if symbol != DEFAULT_SYMBOL:
            return None
        bars = self._atr_bars[-count:]
        base = self.server_epoch() - (len(bars) + 1) * 300
        rates = [
            {
                "time": base + index * 300,
                "open": values[0],
                "high": values[1],
                "low": values[2],
                "close": values[3],
            }
            for index, values in enumerate(bars)
        ]
        # MT5 returns present-to-past; the adapter reverses to oldest-first.
        return list(reversed(rates))

    # -- Terminal writes --------------------------------------------------

    def order_send(self, request: dict[str, Any]) -> SimpleNamespace:
        self.sends.append(dict(request))
        if self.reject_next_send:
            self.reject_next_send = False
            return _order_result(request, retcode=TRADE_RETCODE_REJECT, comment="rejected")
        if request.get("action") == TRADE_ACTION_SLTP:
            leg = self.legs.get(int(request["position"]))
            if leg is not None:
                leg.take_profit = float(request["tp"])
                leg.stop_loss = float(request["sl"])
        return _order_result(request)
