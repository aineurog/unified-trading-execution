"""Shared fixtures for Hyperliquid integration tests (testnet only).

Integration tests run against testnet with a faucet-funded address and never
against mainnet. Credentials come from the environment — ``HYPERLIQUID_TESTNET_ADDRESS``
and ``HYPERLIQUID_TESTNET_PRIVATE_KEY`` — with packaged ``.env`` files picked up
automatically (Bybit precedent: a real environment always wins over files).
Every fixture skips (never fails) when credentials are missing, so CI and local
checkouts without secrets stay green.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import AsyncIterator, Callable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import TypeVar

import pytest

from unified_trading_execution.events import Event, EventBus
from unified_trading_execution.hyperliquid import (
    HyperliquidAdapter,
    HyperliquidConfig,
    HyperliquidEngine,
    SyncHyperliquidEngine,
)
from unified_trading_execution.types.enums import AssetClass
from unified_trading_execution.types.instrument import Instrument


def _load_env_files() -> None:
    """Load any packaged ``.env`` files into the process environment.

    Only sets a variable if it is not already set, so a real environment
    (shell export / CI secret / ``uv run --env-file``) always wins.
    """
    lookups = [
        Path(__file__).resolve().parents[2],  # packages/adapter-hyperliquid/
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
def hyperliquid_testnet_address() -> str:
    return _require_env("HYPERLIQUID_TESTNET_ADDRESS")


@pytest.fixture(scope="session")
def hyperliquid_testnet_private_key() -> str:
    return _require_env("HYPERLIQUID_TESTNET_PRIVATE_KEY")


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def hyperliquid_config(
    hyperliquid_testnet_address: str,
    hyperliquid_testnet_private_key: str,
) -> HyperliquidConfig:
    return HyperliquidConfig(
        wallet_address=hyperliquid_testnet_address,
        private_key=hyperliquid_testnet_private_key,
        testnet=True,
    )


@pytest.fixture
async def connected_adapter(
    hyperliquid_config: HyperliquidConfig,
    event_bus: EventBus,
) -> AsyncIterator[HyperliquidAdapter]:
    """A HyperliquidAdapter connected to testnet — disconnected after the test.

    Attaches an initialized in-memory store first: leverage/margin intent
    persistence requires one, and outside the engine nothing provides it
    (the engine auto-creates a SQLite store at connect — engine.py:229-236).
    """
    from unified_trading_execution.state.store import SQLiteStateStore

    adapter = HyperliquidAdapter(hyperliquid_config, event_bus=event_bus)
    store = SQLiteStateStore(":memory:")
    await store.initialize()
    adapter.attach_state_store(store)
    await adapter.connect()
    yield adapter
    with contextlib.suppress(Exception):
        await adapter.disconnect()


@pytest.fixture
async def connected_engine(
    hyperliquid_config: HyperliquidConfig,
) -> AsyncIterator[HyperliquidEngine]:
    """A HyperliquidEngine connected to testnet (REST + streams) — shut down after."""
    engine = HyperliquidEngine(hyperliquid_config)
    await engine.connect()
    yield engine
    with contextlib.suppress(Exception):
        await engine.ashutdown()


def _perp(coin: str) -> Instrument:
    return Instrument(
        symbol=coin,
        quote_currency="USDC",
        asset_class=AssetClass.FUTURES,
        currency="USDC",
        multiplier=1,
    )


@pytest.fixture
def btc_perp() -> Instrument:
    """BTC perp instrument (venue-known — no discovery needed)."""
    return _perp("BTC")


@pytest.fixture
def eth_perp() -> Instrument:
    """ETH perp instrument (venue-known — no discovery needed)."""
    return _perp("ETH")


@pytest.fixture
async def reference_price(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
) -> Decimal:
    """Live mid price for BTC — safe anchor for resting limits and TP/SL math."""
    ticker = await connected_adapter.fetch_ticker(btc_perp)
    mid = ticker.mid if ticker is not None else None
    if mid is None or mid <= 0:
        pytest.skip("No live BTC quote on testnet")
    assert isinstance(mid, Decimal)
    return mid


@pytest.fixture
def unique_cid() -> Callable[[str], str]:
    """Factory for run-unique client order ids — no cross-test collisions."""
    import time
    import uuid

    tag = f"{time.strftime('%H%M%S')}-{uuid.uuid4().hex[:8]}"
    counter = 0

    def _make(prefix: str) -> str:
        nonlocal counter
        counter += 1
        return f"hlit-{tag}-{prefix}-{counter}"

    return _make


@pytest.fixture
async def funded_account(connected_adapter: HyperliquidAdapter) -> Decimal:
    """Guard: skip unless the testnet account holds enough USDC to trade."""
    balances = await connected_adapter.fetch_balances()
    usdc = balances.get("USDC")
    total = usdc.total if usdc is not None else Decimal("0")
    if total < Decimal("50"):
        pytest.skip(f"Testnet account underfunded for live trading (USDC={total})")
    return total


@pytest.fixture
async def flattened_book(
    connected_adapter: HyperliquidAdapter,
    btc_perp: Instrument,
) -> AsyncIterator[None]:
    """Pre- and post-test flatten: cancel all opens, close all legs on BTC.

    Makes every trading test idempotent — a prior aborted run can never break
    the next one, and no test leaks state. Silent-tolerant by design.
    """
    from .helpers import flatten_all

    await flatten_all(connected_adapter)
    yield
    await flatten_all(connected_adapter)


_TEvent = TypeVar("_TEvent", bound=Event)


class EventCollector:
    """Subscribe to event types on the bus and drain captured events."""

    def __init__(self, event_bus: EventBus, *event_types: type[Event]) -> None:
        self._events: list[Event] = []
        for event_type in event_types or (Event,):
            event_bus.subscribe(event_type, self._on_event)

    def _on_event(self, event: Event) -> None:
        self._events.append(event)

    def drain(self) -> list[Event]:
        events, self._events = self._events, []
        return events

    def __len__(self) -> int:
        return len(self._events)

    def events_since(self, index: int) -> list[Event]:
        """Peek at events captured after ``index`` without draining."""
        return list(self._events[index:])

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
def sync_engine_factory(
    hyperliquid_config: HyperliquidConfig,
) -> Iterator[Callable[[], SyncHyperliquidEngine]]:
    """Factory for blocking engines with guaranteed shutdown."""
    engines: list[SyncHyperliquidEngine] = []

    def _make() -> SyncHyperliquidEngine:
        engine = SyncHyperliquidEngine(hyperliquid_config)
        engines.append(engine)
        return engine

    yield _make
    for engine in engines:
        with contextlib.suppress(Exception):
            engine.shutdown()
