"""REST rate-limit observations for Binance products."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import TypeAlias

from unified_trading_execution.adapter import RateLimits
from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.errors import PlatformError

_HEADER_PATTERN = re.compile(r"^x-mbx-(used-weight|order-count)-(\d+)([smhd])$")
_INTERVAL_SECONDS = {
    "S": 1,
    "M": 60,
    "H": 60 * 60,
    "D": 24 * 60 * 60,
}
_INTERVAL_LETTERS = {
    "SECOND": "S",
    "MINUTE": "M",
    "HOUR": "H",
    "DAY": "D",
}
_OBSERVED_LIMIT_TYPES = frozenset({"REQUEST_WEIGHT", "ORDERS", "RAW_REQUESTS"})
_MAX_RETRY_AFTER_SECONDS = 3 * 24 * 60 * 60
_UNKNOWN_LIMIT_INTERVAL_SECONDS = 60.0

_RateLimitKey: TypeAlias = tuple[str, int, str]


@dataclass(frozen=True, slots=True)
class _LimitDefinition:
    rate_limit_type: str
    interval_num: int
    interval_letter: str
    limit: int

    @property
    def key(self) -> _RateLimitKey:
        return (self.rate_limit_type, self.interval_num, self.interval_letter)

    @property
    def interval_seconds(self) -> float:
        return float(self.interval_num * _INTERVAL_SECONDS[self.interval_letter])


@dataclass(frozen=True, slots=True)
class _Usage:
    used: int | None
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class _Candidate:
    limit: int
    remaining: int
    interval_seconds: float
    reset_at: datetime


class BinanceRateLimitTracker:
    """Track observable REST counters and expose a coarse core budget.

    Binance has independent request-weight and order-count windows. The core
    ``RateLimits`` contract represents only one unweighted budget, so this
    tracker reports the observed bucket with the least remaining count. Since
    these counts have different meanings and request costs, this is not
    guaranteed to be conservative for every operation. ``RAW_REQUESTS`` is
    counted locally because Binance does not expose a corresponding response
    header. Only requests routed through this tracker are counted, and usage
    from other processes sharing the same IP is not observable. Request
    weights for individual endpoints and WebSocket limits remain outside this
    core budget.
    """

    def __init__(self) -> None:
        self._definitions: dict[BinanceProduct, dict[_RateLimitKey, _LimitDefinition]] = {}
        self._usage: dict[BinanceProduct, dict[_RateLimitKey, _Usage]] = {}
        self._raw_request_counts: dict[BinanceProduct, dict[_RateLimitKey, _Usage]] = {}
        self._unparseable_header_until: dict[BinanceProduct, datetime] = {}
        self._blocked_until: datetime | None = None

    def update(
        self,
        product: BinanceProduct,
        *,
        headers: Mapping[str, str] | None,
        rate_limits: object = None,
        status_code: int | None = None,
        now: datetime | None = None,
    ) -> None:
        """Record one SDK response, retaining prior counters omitted by it."""
        moment = now or datetime.now(UTC)
        if rate_limits is not None:
            definitions = _parse_definitions(rate_limits)
            if definitions:
                self._definitions[product] = {
                    definition.key: definition for definition in definitions
                }

        product_definitions = self._definitions.get(product, {})
        raw_counts = self._raw_request_counts.setdefault(product, {})
        for definition in product_definitions.values():
            if definition.rate_limit_type != "RAW_REQUESTS":
                continue
            previous = raw_counts.get(definition.key)
            if previous is None or moment >= previous.observed_at + timedelta(
                seconds=definition.interval_seconds
            ):
                raw_counts[definition.key] = _Usage(1, moment)
            else:
                raw_counts[definition.key] = _Usage(
                    (previous.used or 0) + 1,
                    previous.observed_at,
                )

        usage = self._usage.setdefault(product, {})
        for name, raw_value in (headers or {}).items():
            lowered_name = name.lower()
            match = _HEADER_PATTERN.fullmatch(lowered_name)
            if match is None:
                if lowered_name.startswith(("x-mbx-used-weight-", "x-mbx-order-count-")):
                    self._fail_closed_on_unparseable_header(
                        product,
                        moment,
                        product_definitions,
                    )
                continue
            header_type, raw_interval_num, interval_letter = match.groups()
            rate_limit_type = "REQUEST_WEIGHT" if header_type == "used-weight" else "ORDERS"
            interval_num = _positive_int(raw_interval_num)
            if (
                interval_num is None
                or interval_num
                > _MAX_RETRY_AFTER_SECONDS // _INTERVAL_SECONDS[interval_letter.upper()]
            ):
                self._fail_closed_on_unparseable_header(
                    product,
                    moment,
                    product_definitions,
                )
                continue
            key = (rate_limit_type, interval_num, interval_letter.upper())
            used = _nonnegative_int(raw_value)
            usage[key] = _Usage(used, moment)

        if status_code in (418, 429):
            retry_after = _header_value(headers or {}, "Retry-After")
            delay = _retry_after_seconds(retry_after)
            if delay is None:
                if status_code == 418:
                    delay = _MAX_RETRY_AFTER_SECONDS
                else:
                    delay = max(
                        (
                            definition.interval_seconds
                            for definition in product_definitions.values()
                        ),
                        default=_UNKNOWN_LIMIT_INTERVAL_SECONDS,
                    )
            blocked_until = moment + timedelta(seconds=delay)
            if self._blocked_until is None or blocked_until > self._blocked_until:
                self._blocked_until = blocked_until

    def _fail_closed_on_unparseable_header(
        self,
        product: BinanceProduct,
        now: datetime,
        definitions: Mapping[_RateLimitKey, _LimitDefinition],
    ) -> None:
        reset_after = max(
            (definition.interval_seconds for definition in definitions.values()),
            default=_UNKNOWN_LIMIT_INTERVAL_SECONDS,
        )
        reset_at = now + timedelta(seconds=reset_after)
        previous = self._unparseable_header_until.get(product)
        if previous is None or reset_at > previous:
            self._unparseable_header_until[product] = reset_at

    def snapshot(self, *, now: datetime | None = None) -> RateLimits:
        """Return the tightest observed REST budget, never a success fallback."""
        moment = now or datetime.now(UTC)
        candidates: list[_Candidate] = []

        products = self._definitions.keys() | self._usage.keys()
        for product in products:
            definitions = self._definitions.get(product, {})
            usage_by_key = self._usage.get(product, {})
            raw_by_key = self._raw_request_counts.get(product, {})
            for definition in definitions.values():
                if definition.rate_limit_type == "RAW_REQUESTS":
                    usage = raw_by_key.get(definition.key)
                elif definition.rate_limit_type in ("REQUEST_WEIGHT", "ORDERS"):
                    usage = usage_by_key.get(definition.key)
                else:
                    continue

                # A bucket with no observed counter cannot constrain the
                # budget yet. Counting it as one available request would let
                # an unseen window such as a daily order cap dominate the
                # snapshot and block every product for that whole interval.
                if usage is None:
                    continue

                interval_end = usage.observed_at + timedelta(seconds=definition.interval_seconds)
                if moment >= interval_end:
                    remaining = definition.limit
                    reset_at = moment + timedelta(seconds=definition.interval_seconds)
                elif usage.used is None:
                    remaining = 0
                    reset_at = interval_end
                else:
                    remaining = max(definition.limit - usage.used, 0)
                    reset_at = interval_end
                candidates.append(
                    _Candidate(
                        limit=definition.limit,
                        remaining=remaining,
                        interval_seconds=definition.interval_seconds,
                        reset_at=reset_at,
                    )
                )

            for key, usage in usage_by_key.items():
                if key in definitions:
                    continue
                _, interval_num, interval_letter = key
                interval_seconds = float(interval_num * _INTERVAL_SECONDS.get(interval_letter, 60))
                candidates.append(
                    _Candidate(
                        limit=1,
                        remaining=0,
                        interval_seconds=interval_seconds,
                        reset_at=usage.observed_at + timedelta(seconds=interval_seconds),
                    )
                )

            unparseable_until = self._unparseable_header_until.get(product)
            if unparseable_until is not None and unparseable_until > moment:
                candidates.append(
                    _Candidate(
                        limit=1,
                        remaining=0,
                        interval_seconds=(unparseable_until - moment).total_seconds(),
                        reset_at=unparseable_until,
                    )
                )

        if self._blocked_until is not None and self._blocked_until > moment:
            candidates.append(
                _Candidate(
                    limit=1,
                    remaining=0,
                    interval_seconds=(self._blocked_until - moment).total_seconds(),
                    reset_at=self._blocked_until,
                )
            )

        if not candidates:
            return RateLimits(
                requests_per_interval=1,
                interval_seconds=_UNKNOWN_LIMIT_INTERVAL_SECONDS,
                remaining=0,
                reset_at=moment,
            )

        most_restrictive = min(
            candidates,
            key=lambda candidate: (candidate.remaining, -candidate.interval_seconds),
        )
        return RateLimits(
            requests_per_interval=max(most_restrictive.limit, 1),
            interval_seconds=most_restrictive.interval_seconds,
            remaining=most_restrictive.remaining,
            reset_at=most_restrictive.reset_at,
        )


def _parse_definitions(raw_limits: object) -> tuple[_LimitDefinition, ...]:
    if not isinstance(raw_limits, list):
        return ()

    definitions: list[_LimitDefinition] = []
    for raw_definition in raw_limits:
        if not isinstance(raw_definition, Mapping):
            raise PlatformError("Binance exchange info contains a malformed rate-limit entry")
        rate_limit_type = raw_definition.get("rateLimitType")
        if not isinstance(rate_limit_type, str):
            raise PlatformError("Binance exchange info contains a malformed rate-limit type")
        if rate_limit_type not in _OBSERVED_LIMIT_TYPES:
            continue
        interval = raw_definition.get("interval")
        interval_num = _positive_int(raw_definition.get("intervalNum"))
        limit = _positive_int(raw_definition.get("limit"))
        if not isinstance(interval, str) or interval.upper() not in _INTERVAL_LETTERS:
            raise PlatformError(
                f"Binance exchange info has an invalid {rate_limit_type} rate-limit interval"
            )
        if interval_num is None or limit is None:
            raise PlatformError(
                f"Binance exchange info has an invalid {rate_limit_type} rate-limit value"
            )
        if (
            interval_num
            > _MAX_RETRY_AFTER_SECONDS // _INTERVAL_SECONDS[_INTERVAL_LETTERS[interval.upper()]]
        ):
            raise PlatformError(
                f"Binance exchange info has an excessive {rate_limit_type} interval"
            )
        definitions.append(
            _LimitDefinition(
                rate_limit_type=rate_limit_type,
                interval_num=interval_num,
                interval_letter=_INTERVAL_LETTERS[interval.upper()],
                limit=limit,
            )
        )
    return tuple(definitions)


def _nonnegative_int(raw_value: object) -> int | None:
    if isinstance(raw_value, bool):
        return None
    if isinstance(raw_value, int):
        return raw_value if raw_value >= 0 else None
    if not isinstance(raw_value, str):
        return None
    normalized = raw_value.strip()
    try:
        parsed = int(normalized)
    except ValueError:
        return None
    if str(parsed) != normalized or parsed < 0:
        return None
    return parsed


def _positive_int(raw_value: object) -> int | None:
    parsed = _nonnegative_int(raw_value)
    if parsed is None or parsed == 0:
        return None
    return parsed


def _header_value(headers: Mapping[str, str], key: str) -> str | None:
    direct_value = headers.get(key)
    if direct_value is not None:
        return direct_value
    lowered_key = key.lower()
    for name, value in headers.items():
        if name.lower() == lowered_key:
            return value
    return None


def _retry_after_seconds(raw_value: str | None) -> float | None:
    if raw_value is None:
        return None
    try:
        seconds = Decimal(raw_value.strip())
    except (InvalidOperation, ValueError):
        return None
    if not seconds.is_finite() or seconds < 0:
        return None
    return min(float(seconds), float(_MAX_RETRY_AFTER_SECONDS))
