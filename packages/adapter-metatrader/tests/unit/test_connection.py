"""Unit tests for MT5 connection lifecycle.

Tests cases:
    - connect: acquires process-global guard, calls mt5.initialize()
    - connect: raises PlatformConnectionError if mt5.initialize() returns False
    - connect: raises PlatformConnectionError if already connected in this process
    - connect: raises PlatformConnectionError if account_info() returns None
    - connect: starts polling loop as background task
    - disconnect: cancels polling loop, calls mt5.shutdown(), releases guard
    - disconnect: publishes ConnectionStateEvent(connected=False)
    - disconnect: is idempotent (safe to call when already disconnected)
    - is_connected: reflects current state
    - Process-global guard: second adapter in same process is blocked
"""

from __future__ import annotations


class TestConnect:
    """MT5Adapter.connect() lifecycle."""

    def test_successful_connection(self) -> None:
        """Acquires guard, initializes terminal, starts polling."""
        ...

    def test_initialize_failure(self) -> None:
        """mt5.initialize() returns False → PlatformConnectionError."""
        ...

    def test_account_info_none(self) -> None:
        """account_info() returns None → PlatformConnectionError."""
        ...

    def test_already_connected_in_process(self) -> None:
        """Second adapter in same process raises PlatformConnectionError."""
        ...

    def test_starts_polling_loop(self) -> None:
        """connect() creates an asyncio background task for _poll_loop."""
        ...


class TestDisconnect:
    """MT5Adapter.disconnect() lifecycle."""

    def test_cancels_polling_and_shuts_down(self) -> None:
        """Polling loop cancelled, mt5.shutdown() called, guard released."""
        ...

    def test_publishes_disconnection_event(self) -> None:
        """disconnect() publishes ConnectionStateEvent(connected=False)."""
        ...

    def test_idempotent(self) -> None:
        """Calling disconnect twice is safe."""
        ...
