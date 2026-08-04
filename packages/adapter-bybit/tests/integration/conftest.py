"""Shared fixtures for Bybit adapter integration tests.

Integration tests connect to the real Bybit testnet.  They are skipped
(not failed) when credentials are missing so that CI and local checkouts
without API keys stay green.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path
from typing import Any, TypeVar, cast

import pytest

from unified_trading_execution.bybit import BybitAdapter, BybitConfig
from unified_trading_execution.bybit.symbols import from_bybit_symbol
from unified_trading_execution.events import Event, EventBus
from unified_trading_execution.types.instrument import Instrument

_ORDER_CATEGORIES: tuple[str, ...] = ("spot", "linear", "inverse")

# Preferred symbols per category — used when present on testnet, with live
# discovery as the fallback so tests never hard-code a symbol that may delist.
_PREFERRED_SYMBOLS: dict[str, tuple[str, ...]] = {
    "spot": ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"),
    "linear": ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"),
    "inverse": ("ETHUSD", "BTCUSD", "XRPUSD", "EOSUSD", "LTCUSD"),
}


def _load_env_files() -> None:
    """Load any packaged ``.env`` files into the process environment.

    Only sets a variable if it is not already set, so a real environment
    (shell export / CI secret / ``uv run --env-file``) always wins.  This
    makes tests pick up ``packages/adapter-bybit/.env`` (and a repo-root
    ``.env``) without needing python-dotenv installed.
    """
    lookups = [
        Path(__file__).resolve().parents[2],  # packages/adapter-bybit/
        Path(__file__).resolve().parents[4],  # repo root
    ]
    seen: set[Path] = set()
    for directory in lookups:
        env_file = directory / ".env"
        if env_file in seen or not env_file.is_file():
            continue
        seen.add(env_file)
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


_load_env_files()


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.skip(f"{name} not set — skipping integration test")
    return value


@pytest.fixture(scope="session")
def bybit_testnet_api_key() -> str:
    return _require_env("BYBIT_TESTNET_API_KEY")


@pytest.fixture(scope="session")
def bybit_testnet_api_secret() -> str:
    return _require_env("BYBIT_TESTNET_API_SECRET")


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def bybit_config(
    bybit_testnet_api_key: str,
    bybit_testnet_api_secret: str,
) -> BybitConfig:
    return BybitConfig(
        api_key=bybit_testnet_api_key,
        api_secret=bybit_testnet_api_secret,
        testnet=True,
    )


@pytest.fixture
async def connected_adapter(
    bybit_config: BybitConfig,
    event_bus: EventBus,
) -> AsyncIterator[BybitAdapter]:
    """A BybitAdapter connected to testnet — cleaned up after the test."""
    adapter = BybitAdapter(bybit_config, event_bus=event_bus)
    await adapter.connect()
    yield adapter
    with contextlib.suppress(Exception):
        await adapter.disconnect()


@pytest.fixture(params=_ORDER_CATEGORIES)
def category(request: pytest.FixtureRequest) -> str:
    """Parametrized Bybit product category: spot, linear, or inverse."""
    return str(request.param)


async def _discover_instrument(
    adapter: BybitAdapter,
    category: str,
) -> Instrument | None:
    """Return a live, tradable Instrument for ``category`` or None.

    Queries ``get_instruments_info`` for the category and prefers a
    ``_PREFERRED_SYMBOLS`` entry with ``status == "Trading"`` and a sensible
    min-notional; falls back to any trading symbol.
    """
    data, _ = await adapter._run_request(
        adapter._session.get_instruments_info,
        category=category,
    )
    listings: list[dict[str, Any]] = (data.get("result") or {}).get("list") or []

    def _candidate_score(listing: dict[str, Any]) -> int:
        symbol = listing.get("symbol") or ""
        if listing.get("status") != "Trading":
            return -1
        preferred = _PREFERRED_SYMBOLS.get(category, ())
        if symbol in preferred:
            return 100 + len(preferred) - preferred.index(symbol)
        return 1

    best = max(listings, key=_candidate_score, default=None)
    if best is None:
        return None
    try:
        return cast(
            Instrument,
            from_bybit_symbol(
                str(best.get("symbol") or ""),
                str(best.get("baseCoin") or ""),
                str(best.get("quoteCoin") or ""),
                category,
            ),
        )
    except Exception:
        return None


@pytest.fixture
async def traded_instrument(
    connected_adapter: BybitAdapter,
    category: str,
) -> Instrument:
    """A live, tradable Instrument for the parametrized ``category``."""
    instrument = await _discover_instrument(connected_adapter, category)
    if instrument is None:
        pytest.skip(f"No trading {category} symbol available on testnet")
    return instrument


@pytest.fixture
async def linear_instrument(connected_adapter: BybitAdapter) -> Instrument:
    """A live linear (USDT perp) instrument, or skip."""
    instrument = await _discover_instrument(connected_adapter, "linear")
    if instrument is None:
        pytest.skip("No trading linear symbol available on testnet")
    return instrument


@pytest.fixture
async def spot_instrument(connected_adapter: BybitAdapter) -> Instrument:
    """A live spot instrument, or skip."""
    instrument = await _discover_instrument(connected_adapter, "spot")
    if instrument is None:
        pytest.skip("No trading spot symbol available on testnet")
    return instrument


async def _reference_price(
    adapter: BybitAdapter,
    instrument: Instrument,
) -> Decimal:
    """Return a mid price from the live order book — safe for a resting limit."""
    category = adapter._instrument_to_category(instrument)
    symbol = f"{instrument.symbol}{instrument.quote_currency}"
    data, _ = await adapter._run_request(
        adapter._session.get_orderbook,
        category=category,
        symbol=symbol,
        limit=1,
    )
    result = data.get("result") or {}
    bids = result.get("b") or []
    asks = result.get("a") or []
    if not bids or not asks:
        raise AssertionError(f"Empty order book for {symbol}")
    bid = Decimal(str(bids[0][0]))
    ask = Decimal(str(asks[0][0]))
    return (bid + ask) / Decimal("2")


@pytest.fixture
async def reference_price(
    connected_adapter: BybitAdapter,
    traded_instrument: Instrument,
) -> Decimal:
    """Live mid price for ``traded_instrument`` (used for resting limit orders)."""
    return await _reference_price(connected_adapter, traded_instrument)


@pytest.fixture
async def linear_reference_price(
    connected_adapter: BybitAdapter,
    linear_instrument: Instrument,
) -> Decimal:
    """Live mid price for the linear instrument (used for resting limit orders)."""
    return await _reference_price(connected_adapter, linear_instrument)


async def cleanup_open_orders(adapter: BybitAdapter) -> None:
    """Best-effort cancel of every open order — no cross-test pollution."""
    open_orders = await adapter.fetch_open_orders()
    for client_order_id in open_orders:
        with contextlib.suppress(Exception):
            await adapter.cancel_order(client_order_id)


_TEvent = TypeVar("_TEvent", bound=Event)


class EventCollector:
    """Subscribe to event types on the bus and drain captured events."""

    def __init__(self, event_bus: EventBus, *event_types: type[Event]) -> None:
        self._events: list[Event] = []
        self._event_types = event_types or (Event,)
        for event_type in self._event_types:
            event_bus.subscribe(event_type, self._on_event)

    def _on_event(self, event: Event) -> None:
        self._events.append(event)

    def drain(self) -> list[Event]:
        events, self._events = self._events, []
        return events

    def of_type(self, event_type: type[_TEvent]) -> list[_TEvent]:
        return [event for event in self._events if isinstance(event, event_type)]

    async def wait_for(
        self,
        event_type: type[_TEvent],
        *,
        count: int = 1,
        timeout: float = 30.0,
    ) -> list[_TEvent]:
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            matching = self.of_type(event_type)
            if len(matching) >= count:
                return matching
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for {count}x {event_type.__name__}; "
                    f"captured {len(self._events)} events"
                )
            await asyncio.sleep(0.1)


@pytest.fixture
def collect_events(event_bus: EventBus) -> EventCollector:
    """Collector subscribing to all event types on the bus."""
    return EventCollector(event_bus)


@pytest.fixture
async def responsive_loop_probe() -> AsyncIterator["LoopProbe"]:
    """Prove the event loop stays responsive while WS messages stream in.

    Yields an asyncio task that bumps a counter every 20ms; the test asserts
    the counter advanced within a deadline during/after stream activity.
    """

    probe = LoopProbe()
    yield probe
    await probe.stop()


class LoopProbe:
    """A counters task that proves the event loop is not blocked."""

    def __init__(self) -> None:
        self.ticks = 0
        self._running = False
        self._task: asyncio.Task[None] | None = None

    async def _run(self) -> None:
        while self._running:
            self.ticks += 1
            await asyncio.sleep(0.02)

    def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
