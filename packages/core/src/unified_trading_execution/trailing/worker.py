"""Polling worker for synthetic trailing states.

One worker serves every synthetic state on an engine.  Each cycle groups states
by instrument, samples each instrument at most once, and hands the snapshot to
the manager, which owns ratcheting, persistence, and failure policy.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from unified_trading_execution.adapter import Adapter
from unified_trading_execution.errors import RateLimitError
from unified_trading_execution.trailing.manager import TrailingManager
from unified_trading_execution.trailing.math import wilder_atr
from unified_trading_execution.trailing.types import (
    CandleTimeframe,
    TrailingConfig,
    TrailingMode,
    TrailingPhase,
    TrailingState,
)
from unified_trading_execution.types.instrument import Instrument

logger = logging.getLogger(__name__)

_POLLED_PHASES = frozenset({TrailingPhase.ARMED, TrailingPhase.DEGRADED})
_MAX_BACKOFF_SECONDS = 60.0
_TIMEFRAME_SECONDS: dict[CandleTimeframe, int] = {
    CandleTimeframe.M1: 60,
    CandleTimeframe.M5: 300,
    CandleTimeframe.M15: 900,
    CandleTimeframe.M30: 1800,
    CandleTimeframe.H1: 3600,
    CandleTimeframe.H4: 14400,
    CandleTimeframe.H8: 28800,
    CandleTimeframe.H12: 43200,
    CandleTimeframe.D1: 86400,
}


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _next_candle_close(
    as_of: datetime,
    timeframe: CandleTimeframe,
    settle_seconds: float,
) -> datetime:
    seconds = _TIMEFRAME_SECONDS[timeframe]
    next_epoch = (int(as_of.timestamp()) // seconds + 1) * seconds
    return datetime.fromtimestamp(next_epoch, tz=UTC) + timedelta(seconds=settle_seconds)


class TrailingWorker:
    """One polling task per engine, with no queueing or per-state tasks."""

    def __init__(
        self,
        manager: TrailingManager,
        adapter: Adapter,
        config: TrailingConfig | None = None,
        *,
        clock: Callable[[], float] | None = None,
        wall_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._manager = manager
        self._adapter = adapter
        self._config = config or TrailingConfig()
        self._clock = clock or time.monotonic
        self._wall_clock = wall_clock or _utcnow
        self._last_fetch: dict[Instrument, float] = {}
        self._backoff_until: dict[Instrument, float] = {}
        self._backoff_attempts: dict[Instrument, int] = {}
        self._next_atr_refresh: dict[tuple[Instrument, CandleTimeframe], datetime] = {}
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        """Start the worker task.  Construction never starts it."""
        if self.running:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="trailing-worker")

    async def stop(self) -> None:
        """Signal the worker and await its task."""
        self._stop.set()
        task = self._task
        self._task = None
        if task is not None:
            await task

    async def run_once(self, *, now: float | None = None) -> None:
        """Run one polling cycle.  Exposed for deterministic tests."""
        current = self._clock() if now is None else now
        states = [
            state
            for state in await self._manager.list(active_only=True)
            if state.mode is TrailingMode.SYNTHETIC and state.phase in _POLLED_PHASES
        ]
        if not states:
            return

        atr_states = [state for state in states if state.spec.uses_atr]
        if atr_states:
            await self._refresh_atr(atr_states, self._wall_clock())

        groups: defaultdict[Instrument, list[TrailingState]] = defaultdict(list)
        for state in states:
            groups[state.instrument].append(state)

        semaphore = asyncio.Semaphore(self._config.max_concurrent_fetches)

        async def poll(instrument: Instrument, group: list[TrailingState]) -> None:
            async with semaphore:
                await self._poll_instrument(instrument, group, current)

        await asyncio.gather(
            *(
                poll(instrument, group)
                for instrument, group in groups.items()
                if self._due(instrument, current)
            )
        )

    def _due(self, instrument: Instrument, now: float) -> bool:
        backoff = self._backoff_until.get(instrument)
        if backoff is not None and now < backoff:
            return False
        capability = self._adapter.market_data_capability(instrument)
        interval = max(
            self._config.poll_interval_seconds,
            capability.min_ticker_interval_seconds,
        )
        last = self._last_fetch.get(instrument)
        return last is None or now - last >= interval

    async def _refresh_atr(
        self,
        states: list[TrailingState],
        wall_now: datetime,
    ) -> None:
        groups: defaultdict[tuple[Instrument, CandleTimeframe], list[TrailingState]] = defaultdict(
            list
        )
        for state in states:
            timeframe = state.spec.atr_timeframe
            if timeframe is None or state.spec.atr_period is None:
                continue
            groups[(state.instrument, timeframe)].append(state)

        semaphore = asyncio.Semaphore(self._config.max_concurrent_fetches)

        async def refresh(
            key: tuple[Instrument, CandleTimeframe],
            group: list[TrailingState],
        ) -> None:
            async with semaphore:
                await self._refresh_atr_group(key, group, wall_now)

        await asyncio.gather(*(refresh(key, group) for key, group in groups.items()))

    async def _refresh_atr_group(
        self,
        key: tuple[Instrument, CandleTimeframe],
        states: list[TrailingState],
        wall_now: datetime,
    ) -> None:
        instrument, timeframe = key
        next_refresh = self._next_atr_refresh.get(key)
        if next_refresh is None:
            as_of = max(
                (state.atr_as_of or wall_now for state in states),
                default=wall_now,
            )
            next_refresh = _next_candle_close(as_of, timeframe, self._config.candle_settle_seconds)
            self._next_atr_refresh[key] = next_refresh
        if wall_now < next_refresh:
            return

        periods = {state.spec.atr_period for state in states if state.spec.atr_period is not None}
        if not periods:
            return
        try:
            candles = await self._adapter.fetch_candles(
                instrument,
                timeframe,
                limit=max(periods) + 1,
            )
        except Exception as exc:
            reason = str(exc) or type(exc).__name__
            for state in states:
                await self._manager.on_atr_failure(state.trailing_id, reason)
            self._next_atr_refresh[key] = _next_candle_close(
                wall_now, timeframe, self._config.candle_settle_seconds
            )
            return

        by_period: defaultdict[int, list[TrailingState]] = defaultdict(list)
        for state in states:
            if state.spec.atr_period is not None:
                by_period[state.spec.atr_period].append(state)

        for period, period_states in by_period.items():
            try:
                atr = wilder_atr(candles, period)
            except Exception as exc:
                reason = str(exc) or type(exc).__name__
                for state in period_states:
                    await self._manager.on_atr_failure(state.trailing_id, reason)
                continue
            for state in period_states:
                await self._manager.on_atr(state.trailing_id, atr, as_of=wall_now)

        self._next_atr_refresh[key] = _next_candle_close(
            wall_now, timeframe, self._config.candle_settle_seconds
        )

    async def _poll_instrument(
        self,
        instrument: Instrument,
        states: list[TrailingState],
        now: float,
    ) -> None:
        self._last_fetch[instrument] = now
        try:
            ticker = await self._adapter.fetch_ticker(instrument)
        except RateLimitError as exc:
            attempt = self._backoff_attempts.get(instrument, 0) + 1
            self._backoff_attempts[instrument] = attempt
            delay = min(
                max(self._config.poll_interval_seconds, 1.0) * (2 ** min(attempt, 6)),
                _MAX_BACKOFF_SECONDS,
            )
            self._backoff_until[instrument] = now + delay
            logger.warning(
                "trailing ticker rate limited for %s; backing off %.1fs: %s",
                instrument.symbol,
                delay,
                exc,
            )
            return
        except Exception as exc:
            for state in states:
                await self._manager.on_ticker_failure(
                    state.trailing_id, str(exc) or type(exc).__name__
                )
            return

        self._backoff_attempts.pop(instrument, None)
        self._backoff_until.pop(instrument, None)
        if ticker is None:
            for state in states:
                await self._manager.on_ticker_failure(state.trailing_id, "no ticker available")
            return

        if self._config.publish_ticker_events:
            self._manager.publish_ticker_event(instrument, ticker)
        for state in states:
            await self._manager.on_ticker(state.trailing_id, ticker)

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception("trailing worker cycle failed")
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._config.poll_interval_seconds
                )
            except TimeoutError:
                continue
