"""Shared value-coercion helpers for the public data types."""

from __future__ import annotations

from decimal import Decimal


def as_decimal(value: Decimal | int | float | str) -> Decimal:
    """Coerce a user-supplied number to Decimal without binary-float artefacts.

    ``Decimal(float)`` preserves the binary representation, so we round-trip
    through ``str`` (e.g. ``as_decimal(0.1)`` == ``Decimal("0.1")``).
    """
    return value if isinstance(value, Decimal) else Decimal(str(value))
