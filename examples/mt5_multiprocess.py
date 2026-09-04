"""Multi-account MetaTrader 5 — one engine per process.

``MetaTrader5.initialize()`` / ``MetaTrader5.shutdown()`` are **process-wide
singletons**: a single process can hold only one live MT5 connection at a
time.  Trading several accounts from one script therefore means running **one
engine per process, one account per engine**, with ``multiprocessing``
providing the process isolation.

Why this matters:

* Each child process gets its own MT5 terminal connection, so two accounts
  never fight over the shared terminal handle.
* Each child also gets its own SQLite state store — the path is derived from
  ``<platform>_<account>`` (see ``default_state_store_path``), so positions,
  balances, and halts for account A can never leak into account B's DB.
* The engine's account-change protection (fatal teardown + persisted
  account halt) stays intact per account.

Usage (Windows — MT5 is Windows-only)::

    python examples/mt5_multiprocess.py

The default task below is a **read-only smoke test** — connect, snapshot
balances/positions, reconcile, shut down — so it is safe to run against a
demo terminal.  To make it a long-running trader, swap the body of
``_run_account`` for a keep-alive loop (see the comment at the end).

Note: this script must be run with the ``if __name__ == "__main__":`` guard.
On Windows, ``multiprocessing`` uses ``spawn``, which re-imports this module
in each child; without the guard the children would recursively spawn.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp

from unified_trading_execution.mt5 import MT5Config, MT5Engine

# One config per account.  ``path`` is optional — omit it to auto-detect the
# installed terminal, or set it explicitly when several terminals are
# installed on the same machine.
ACCOUNTS: list[MT5Config] = [
    MT5Config(
        login=11111111,
        password="change-me",
        server="ICMarkets-Demo",
    ),
    MT5Config(
        login=22222222,
        password="change-me",
        server="ICMarkets-Demo",
    ),
]


def run_account(config: MT5Config) -> str:
    """Spawn entry point: run one account's engine in this child process.

    Must stay a module-level function so ``spawn`` can pickle it by reference.
    Catches its own failure so one bad account (wrong password, terminal not
    logged in, …) never aborts the other processes.
    """
    try:
        return asyncio.run(_run_account(config))
    except Exception as exc:  # pragma: no cover - surface any per-process failure
        return f"account {config.login}: FAILED — {type(exc).__name__}: {exc}"


async def _run_account(config: MT5Config) -> str:
    """One engine's lifecycle for one account: connect → snapshot → reconcile.

    Each process owns this engine and its own SQLite store (auto-derived as
    ``./unified_trading_execution_data/metatrader_<login>.db``).
    """
    engine = MT5Engine(config)
    try:
        await engine.connect()

        balances = await engine.fetch_balances()
        positions = await engine.fetch_positions()
        await engine.reconcile()

        balance_str = (
            ", ".join(f"{currency}={bal.total}" for currency, bal in balances.items()) or "(none)"
        )
        return (
            f"account {config.login}: connected, "
            f"{len(positions)} position(s), balances: {balance_str}, "
            f"store: {engine.state_store.path}"
        )
    finally:
        await engine.ashutdown()


def main() -> None:
    # Explicit ``spawn`` — the default on Windows and the safe choice on POSIX,
    # since each child must import a fresh interpreter (and its own MT5 DLL).
    ctx = mp.get_context("spawn")
    with ctx.Pool(processes=len(ACCOUNTS)) as pool:
        results = pool.map(run_account, ACCOUNTS)

    print("=== multi-account run complete ===")
    for line in results:
        print(line)


if __name__ == "__main__":
    main()
