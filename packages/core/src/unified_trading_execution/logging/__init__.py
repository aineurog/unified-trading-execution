"""Structured logging — JSON logging, audit trail writer, correlation ID propagation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """Base type for all audit-trail records.

    Concrete subtypes: OrderAuditEvent, RiskCheckAuditEvent,
    ReconciliationAuditEvent, HaltAuditEvent — each carrying fields
    specific to the event being recorded.
    """

    event_id: str  # UUID7
    timestamp: datetime  # UTC, timezone-aware
    adapter_name: str
    account_id: str
    correlation_id: str
    event_type: str  # dot-separated, e.g. "order.placed", "risk.rejected"
