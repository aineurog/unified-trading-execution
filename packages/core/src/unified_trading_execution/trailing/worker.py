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

from unified_trading_execution.adapter import Adapter
from unified_trading_execution.errors import RateLimitError
from unified_trading_execution.trailing.manager import TrailingManager
from unified_trading_execution.trailing.types import (
    TrailingConfig,
    TrailingMode,
    TrailingPhase,
    TrailingState,
)
from unified_trading_execution.types.instrument import Instrument

logger = logging.getLogger(__name__)

_POLLED_PHASES = frozenset({TrailingPhase.ARMED, TrailingPhase.DEGRADED})
_MAX_BACKOFF_SECONDS = 60.0


class TrailingWorker:
    """One polling task per engine, with no queueing or per-state tasks."""

    def __init__(
        self,
        manager: TrailingManager,
        adapter: Adapter,
        config: TrailingConfig | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._manager = manager
        self._adapter = adapter
        self._config = config or TrailingConfig()
        self._clock = clock or time.monotonic
        self._last_fetch: dict[Instrument, float] = {}
        self._backoff_until: dict[Instrument, float] = {}
        self._backoff_attempts: dict[Instrument, int] = {}
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
                max(self._config.poll_interval_seconds, 1.0)
                * (2 ** min(attempt, 6)),
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
                await self._manager.on_ticker_failure(
                    state.trailing_id, "no ticker available"
                )
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
