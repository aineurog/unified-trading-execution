"""Example: handling halt events in the integrator's own application.

When using manual-mode halt clearing, the integrator must observe halt events
from the event bus and surface them so a human can decide whether to clear.
"""

from __future__ import annotations

import unified_trading_execution as ute


def on_halt_entered(event: ute.HaltEnteredEvent) -> None:
    print(f"HALT ENTERED: {event.scope} — {event.reason}")
    print(f"  Detail: {event.detail}")
    # In a real application, this would:
    # - Send an alert (Slack, Telegram, email).
    # - Flash a UI indicator.
    # - Pause automated strategies on the affected instrument.


def on_halt_cleared(event: ute.HaltClearedEvent) -> None:
    print(f"HALT CLEARED: {event.scope} — cleared by {event.cleared_by}")
