"""Shared fixtures for MT5 adapter integration tests.

Integration tests connect to a real MetaTrader 5 terminal on this machine.
They are skipped (not failed) when credentials are missing or the MetaTrader5
package is not installed (non-Windows), so CI and local checkouts without
an MT5 terminal stay green.

Important:
    MT5 has no testnet/sandbox.  Integration tests ALWAYS run against a
    real broker account — typically a dedicated demo account.  The account
    must be set to NOT auto-trade and MUST have zero real funds.
"""

from __future__ import annotations

import contextlib
import os
import tempfile
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from unified_trading_execution.events import EventBus
from unified_trading_execution.mt5 import MT5Adapter, MT5Config
from unified_trading_execution.state import SQLiteStateStore


def _load_env_files() -> None:
    """Load any packaged ``.env`` files into the process environment.

    Only sets a variable if it is not already set, so a real environment
    (shell export / CI secret / ``uv run --env-file``) always wins.
    """
    lookups = [
        Path(__file__).resolve().parents[2],  # packages/adapter-metatrader/
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


# Check if MetaTrader5 package is importable — skip all integration
# tests on non-Windows CI/development machines.
try:
    import MetaTrader5 as _mt5  # noqa: F401

    _MT5_AVAILABLE = True
except ImportError:
    _MT5_AVAILABLE = False


@pytest.fixture(scope="session")
def mt5_login() -> int:
    if not _MT5_AVAILABLE:
        pytest.skip("MetaTrader5 package not available on this platform")
    return int(_require_env("MT5_LOGIN"))


@pytest.fixture(scope="session")
def mt5_password() -> str:
    return _require_env("MT5_PASSWORD")


@pytest.fixture(scope="session")
def mt5_server() -> str:
    return _require_env("MT5_SERVER")


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def mt5_config(
    mt5_login: int,
    mt5_password: str,
    mt5_server: str,
) -> MT5Config:
    return MT5Config(
        login=mt5_login,
        password=mt5_password,
        server=mt5_server,
    )


@pytest.fixture
async def connected_adapter(
    mt5_config: MT5Config,
    event_bus: EventBus,
) -> AsyncIterator[MT5Adapter]:
    """An MT5Adapter connected to a real MT5 terminal — cleaned up after.

    A throwaway ``SQLiteStateStore`` is attached so ``connect()`` exercises
    the state-store mapping seeding on every integration test, mirroring how
    the engine wires the adapter (``Engine.__init__`` → ``attach_state_store``).
    """
    store_path = Path(tempfile.gettempdir()) / f"ute_mt5_it_{uuid.uuid4().hex}.db"
    store = SQLiteStateStore(str(store_path))
    await store.initialize()
    adapter = MT5Adapter(mt5_config, event_bus=event_bus)
    adapter.attach_state_store(store)
    await adapter.connect()
    yield adapter
    with contextlib.suppress(Exception):
        await adapter.disconnect()
    with contextlib.suppress(Exception):
        await store.close()
    with contextlib.suppress(Exception):
        store_path.unlink()
