"""Interactive Brokers adapter configuration.

``IBKRConfig`` is a frozen dataclass — type-safe, hashable, and testable.
It is supplied by the user at construction time and never hardcoded by the adapter.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS: float = 86400.0


@dataclass(frozen=True, slots=True)
class IBKRConfig:
    """Immutable configuration for the IBKRAdapter.

    Attributes:
        host: Host name or IP address of the local IB Gateway or TWS.
            Default is ``"127.0.0.1"``.
        port: Socket port configured in TWS/Gateway API settings.
            Typically ``4001`` for live trading and ``4002`` for paper.
        client_id: Unique integer ID for the API connection. Must be unique
            among all connected clients to the same Gateway.
        account: Specific IBKR account ID (e.g., ``"DU123456"``). ``None`` uses
            the default or sole account attached to the Gateway.
        default_exchange: Fallback routing exchange when not specified on an
            Instrument (e.g., ``"SMART"`` for stocks/options or ``"IDEALPRO"``
            for spot FX). Default is ``"SMART"``.
        default_currency: Fallback contract currency when not specified on an
            Instrument. Default is ``"USD"``.
        timeout_seconds: Seconds to wait for connection or blocking responses.
            Default 10.0 s.
        readonly: If ``True``, prevents the adapter from submitting orders.
        instrument_spec_cache_ttl: Seconds a cached ``InstrumentSpec`` is
            trusted before being re-fetched. Default 86400 (one day).
            ``None`` caches indefinitely, relying on invalidation only.
    """

    host: str = "127.0.0.1"
    port: int = 4002
    client_id: int = 1
    account: str | None = None
    default_exchange: str = "SMART"
    default_currency: str = "USD"
    timeout_seconds: float = 10.0
    readonly: bool = False
    instrument_spec_cache_ttl: float | None = DEFAULT_INSTRUMENT_SPEC_CACHE_TTL_SECONDS

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError(
                f"timeout_seconds must be > 0, got {self.timeout_seconds}"
            )

        if self.client_id < 0:
            raise ValueError(
                f"client_id must be a non-negative integer, got {self.client_id}"
            )

        if not self.default_exchange:
            raise ValueError("default_exchange must be a non-empty string")

        if not self.default_currency:
            raise ValueError("default_currency must be a non-empty string")

        ttl = self.instrument_spec_cache_ttl
        if ttl is not None and ttl <= 0:
            raise ValueError(
                f"instrument_spec_cache_ttl must be > 0 or None, got {ttl}"
            )
