"""IP weight accounting for the shared REST budget.

Docs basis (rate-limits-and-user-limits, verified live): all REST requests
share an aggregated **1200 weight per minute per IP** budget. ``exchange``
actions cost ``1 + floor(batch_length / 40)``; ``info`` costs 2 for
``l2Book``/``allMids``/``clearinghouseState``/``orderStatus``/
``spotClearinghouseState``/``exchangeStatus``, 60 for ``userRole``, and 20
for everything else. ``userFills``/``userFillsByTime`` (the only listed
endpoints this adapter calls) add one extra weight per 20 items returned —
read here as ``len // 20`` (the docs don't specify rounding; floor is the
conservative reading that never over-reports spend).

What this module does NOT cover, deliberately:

- WebSocket limits (10 connections, 2000 msgs/min, …) are a separate
  budget on a separate transport — REST accounting excludes WS traffic.
- Address-based action limits (1 request per 1 USDC traded, 10k buffer)
  have no spend signal per call and are only queryable via the
  ``userRateLimit`` info endpoint (weight 20 per query); the adapter does
  not poll it.  A 429 from either limiter surfaces as
  ``RateLimitError`` at the call site.
- The budget is advisory: the adapter records spend and reports it via
  ``get_rate_limits`` for core's self-throttling validator, but never
  blocks or sleeps — throttling policy belongs to the caller.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sized

IP_WEIGHT_BUDGET_PER_MINUTE = 1200
IP_WEIGHT_WINDOW_SECONDS = 60.0

#: SDK ``Info`` method name → request weight.  Names are unique across the
#: ``Info``/``Exchange`` surfaces, so one table keyed by method name suffices.
_INFO_WEIGHTS: dict[str, int] = {
    "l2_snapshot": 2,
    "all_mids": 2,
    "user_state": 2,
    "spot_user_state": 2,
    "query_order_by_cloid": 2,
    "user_role": 60,
}

#: Single-action ``Exchange`` methods (weight 1 — no batch array).
_EXCHANGE_SINGLES = frozenset(
    {
        "order",
        "modify_order",
        "cancel_by_cloid",
        "update_leverage",
        "update_isolated_margin",
    }
)

#: Methods whose responses add ``len(response) // 20`` weight.  Covers every
#: per-20 endpoint in the docs that exists as an SDK method in 0.24.0
#: (``recentTrades``/``twapHistory`` have no SDK method — nothing to key on).
_SURCHARGED_METHODS = frozenset(
    {
        "user_fills",
        "user_fills_by_time",
        "funding_history",
        "user_funding_history",
        "historical_orders",
    }
)

#: ``candlesSnapshot`` instead adds ``len(response) // 60`` weight.
_SURCHARGED_60_METHODS = frozenset({"candles_snapshot"})

#: Weight of a full ``connect()``: ``Exchange(...)`` construction fetches
#: ``meta`` (20) and ``spotMeta`` (20) inside ``Info.__init__``, then the
#: identity checks cost ``userRole`` (60) plus ``userAbstraction`` (20).
#: Recorded once after successful setup; the role/abstraction calls bypass
#: ``_run_exchange`` (no exchange to require yet), so they bill here.
CONNECT_WEIGHT = 120

#: Fallback for SDK methods this table doesn't know (including mocks):
#: the maximum non-``userRole`` weight, so unknown calls can only
#: over-report spend, never hide it.
UNKNOWN_WEIGHT = 20


def request_weight(name: str, *, batch_length: int = 1) -> int:
    """Weight of one SDK call by method name."""
    if name == "bulk_orders":
        return 1 + max(batch_length, 1) // 40
    if name in _EXCHANGE_SINGLES:
        return 1
    return _INFO_WEIGHTS.get(name, UNKNOWN_WEIGHT)


def surcharge_weight(name: str, response: object) -> int:
    """Extra weight from response size (0 unless a listed endpoint returned a list)."""
    if not isinstance(response, list):
        return 0
    if name in _SURCHARGED_60_METHODS:
        return len(response) // 60
    if name in _SURCHARGED_METHODS:
        return len(response) // 20
    return 0


class RateBudget:
    """Sliding-window spend tracker over the 60s IP budget.

    Thread-safe: adapter calls run in worker threads via ``to_thread``.
    ``time_fn`` is injectable for deterministic tests.
    """

    def __init__(
        self,
        *,
        budget: int = IP_WEIGHT_BUDGET_PER_MINUTE,
        window_seconds: float = IP_WEIGHT_WINDOW_SECONDS,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> None:
        self._budget = budget
        self._window = window_seconds
        self._time_fn = time_fn
        self._lock = threading.Lock()
        self._entries: list[tuple[float, int]] = []

    def record(self, weight: int) -> None:
        """Record spent weight now; non-positive weights are ignored."""
        if weight <= 0:
            return
        with self._lock:
            now = self._time_fn()
            self._prune(now)
            self._entries.append((now, weight))

    def _prune(self, now: float) -> None:
        cutoff = now - self._window
        self._entries = [(at, w) for at, w in self._entries if at > cutoff]

    def spent(self) -> int:
        """Weight spent inside the current window."""
        with self._lock:
            self._prune(self._time_fn())
            return sum(w for _, w in self._entries)

    def remaining(self) -> int:
        """Budget left in the current window (floored at 0)."""
        return max(0, self._budget - self.spent())

    def resets_in(self) -> float:
        """Seconds until the oldest recorded spend ages out (0 when nothing tracked).

        With a sliding window there is no single reset instant: this is when
        the budget first starts recovering, not when it returns to full.
        """
        with self._lock:
            now = self._time_fn()
            self._prune(now)
            if not self._entries:
                return 0.0
            oldest = min(at for at, _ in self._entries)
            return max(0.0, oldest + self._window - now)


def describe_call(func: object, args: tuple[object, ...]) -> tuple[str, int]:
    """Derive ``(method name, batch length)`` for an SDK call.

    ``bulk_orders(requests, ...)`` sizes its first argument when possible;
    anything unsized counts as a single action.  Unknown shapes degrade to
    ``("", 1)`` and bill at ``UNKNOWN_WEIGHT``.
    """
    name = getattr(func, "__name__", "")
    if not isinstance(name, str) or not name:
        return "", 1
    batch_length = 1
    if name == "bulk_orders" and args:
        first = args[0]
        if isinstance(first, Sized):
            batch_length = max(len(first), 1)
    return name, batch_length
