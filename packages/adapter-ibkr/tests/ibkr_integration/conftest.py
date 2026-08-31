"""Shared fixtures for IBKR adapter integration tests.

Integration tests connect to a real Interactive Brokers Gateway or TWS
on this machine. They are skipped (not failed) when credentials or ports
are missing, so CI and local checkouts without a running gateway stay green.

Important:
    Integration tests ALWAYS run against a real broker connection.
    Ensure you are connecting to a PAPER TRADING account (typically port 4002
    or 7497) with zero real funds.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import AsyncIterator
from importlib.util import find_spec
from pathlib import Path

import pytest

from unified_trading_execution.events import EventBus
from unified_trading_execution.ibkr import IBKRAdapter, IBKRConfig


def _load_env_files() -> None:
    """Load any packaged ``.env`` files into the process environment.

    Only sets a variable if it is not already set, so a real environment
    (shell export / CI secret / ``uv run --env-file``) always wins.
    """
    lookups = [
        Path(__file__).resolve().parents[2],  # packages/adapter-ibkr/
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
            # Strip inline comments (e.g. IBKR_PORT=7497 # comment) — not inside quotes
            if "#" in line:
                # Only split at # that is not inside quotes — simple: split and keep before #
                line = line.split("#", 1)[0].strip()
                if not line or "=" not in line:
                    continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'").split("#", 1)[0].strip()
            os.environ.setdefault(key, value)


_load_env_files()


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.skip(f"{name} not set — skipping integration test")
    return value


# Check if ib_async package is importable — skip all integration
# tests if the library is not installed in the environment.
_IBKR_AVAILABLE = find_spec("ib_async") is not None


@pytest.fixture(scope="session")
def ibkr_host() -> str:
    if not _IBKR_AVAILABLE:
        pytest.skip("ib_async package not available in this environment")
    return os.getenv("IBKR_HOST", "127.0.0.1")


@pytest.fixture(scope="session")
def ibkr_port() -> int:
    return int(_require_env("IBKR_PORT"))


@pytest.fixture(scope="session")
def ibkr_client_id() -> int:
    return int(os.getenv("IBKR_CLIENT_ID", "1"))


@pytest.fixture(scope="session")
def ibkr_account() -> str:
    return _require_env("IBKR_ACCOUNT")


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus()


@pytest.fixture
def ibkr_config(
    ibkr_host: str,
    ibkr_port: int,
    ibkr_client_id: int,
    ibkr_account: str,
) -> IBKRConfig:
    return IBKRConfig(
        host=ibkr_host,
        port=ibkr_port,
        client_id=ibkr_client_id,
        account=ibkr_account,
    )


@pytest.fixture
async def connected_adapter(
    ibkr_config: IBKRConfig,
    event_bus: EventBus,
) -> AsyncIterator[IBKRAdapter]:
    """An IBKRAdapter connected to a real Gateway/TWS — cleaned up after."""
    adapter = IBKRAdapter(ibkr_config, event_bus=event_bus)
    await adapter.connect()
    yield adapter
    with contextlib.suppress(Exception):
        await adapter.disconnect()
