"""Translate Binance SDK and transport errors to the core exception hierarchy."""

from __future__ import annotations

from aiohttp import ClientError
from binance.exceptions import (  # type: ignore[import-untyped]
    BinanceAPIException,
    BinanceOrderException,
    BinanceRequestException,
)

from unified_trading_execution.binance.enums import BinanceProduct
from unified_trading_execution.errors import (
    DuplicateOrderIdError,
    InsufficientBalanceError,
    InvalidOrderError,
    InvalidSymbolError,
    MarketClosedError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
    RateLimitError,
    UteError,
)

_BINANCE_CODE_MAP: dict[int, type[UteError]] = {
    -1000: PlatformConnectionError,
    -1001: PlatformConnectionError,
    -1002: PlatformError,
    -1003: RateLimitError,
    -1006: PlatformConnectionError,
    -1007: PlatformConnectionError,
    -1014: InvalidOrderError,
    -1013: InvalidOrderError,
    -1015: RateLimitError,
    -1016: PlatformConnectionError,
    -1021: PlatformError,
    -1022: PlatformError,
    -1034: RateLimitError,
    -1100: InvalidOrderError,
    -1101: InvalidOrderError,
    -1102: InvalidOrderError,
    -1103: InvalidOrderError,
    -1104: InvalidOrderError,
    -1105: InvalidOrderError,
    -1106: InvalidOrderError,
    -1108: InvalidOrderError,
    -1111: InvalidOrderError,
    -1114: InvalidOrderError,
    -1115: InvalidOrderError,
    -1116: InvalidOrderError,
    -1117: InvalidOrderError,
    -1118: InvalidOrderError,
    -1119: InvalidOrderError,
    -1121: InvalidSymbolError,
    -1122: InvalidSymbolError,
    -1125: PlatformError,
    -1128: InvalidOrderError,
    -1130: InvalidOrderError,
    -1010: InvalidOrderError,
    -4000: InvalidOrderError,
    -4001: InvalidOrderError,
    -4002: InvalidOrderError,
    -4003: InvalidOrderError,
    -4004: InvalidOrderError,
    -4005: InvalidOrderError,
    -4006: InvalidOrderError,
    -4007: InvalidOrderError,
    -4008: InvalidOrderError,
    -4009: InvalidOrderError,
    -4010: InvalidOrderError,
    -4011: InvalidOrderError,
    -4012: InvalidOrderError,
    -4013: InvalidOrderError,
    -4014: InvalidOrderError,
    -4015: InvalidOrderError,
    -4016: InvalidOrderError,
    -4017: InvalidOrderError,
    -4018: InvalidOrderError,
    -4019: InvalidOrderError,
    -4020: InvalidOrderError,
    -4022: InvalidOrderError,
    -4023: InvalidOrderError,
    -4024: InvalidOrderError,
    -4025: InvalidOrderError,
    -4028: InvalidOrderError,
    -4029: InvalidOrderError,
    -4030: InvalidOrderError,
    -4031: InvalidOrderError,
    -4032: InvalidOrderError,
    -4045: InvalidOrderError,
    -4050: InsufficientBalanceError,
    -4051: InsufficientBalanceError,
    -4060: InvalidOrderError,
    -4061: InvalidOrderError,
    -4062: InvalidOrderError,
    -4105: InvalidOrderError,
    -4106: InvalidOrderError,
    -4107: InvalidOrderError,
    -4114: InvalidOrderError,
    -4111: InvalidOrderError,
    -4110: InvalidOrderError,
    -4112: InvalidOrderError,
    -4113: InvalidOrderError,
    -2016: PlatformError,
    -2018: InsufficientBalanceError,
    -2010: InvalidOrderError,
    -2011: InvalidOrderError,
    -2013: OrderNotFoundError,
    -2014: PlatformError,
    -2015: PlatformError,
    -2019: InsufficientBalanceError,
    -2020: InvalidOrderError,
    -2021: InvalidOrderError,
    -2022: InvalidOrderError,
    -2024: InvalidOrderError,
    -2025: InvalidOrderError,
    -2026: InvalidOrderError,
    -2027: InvalidOrderError,
    -2028: InsufficientBalanceError,
    -2038: InvalidOrderError,
    -2039: InvalidOrderError,
    -2043: InvalidOrderError,
    -4115: InvalidOrderError,
    -4116: DuplicateOrderIdError,
    -4117: InvalidOrderError,
    -4118: InvalidOrderError,
    -4120: InvalidOrderError,
    -4131: InvalidOrderError,
    -4135: InvalidOrderError,
    -4137: InvalidOrderError,
    -4138: InvalidOrderError,
    -4139: InvalidOrderError,
    -4140: MarketClosedError,
    -4141: MarketClosedError,
    -4142: InvalidOrderError,
    -4144: InvalidSymbolError,
    -4164: InvalidOrderError,
}


def _as_int(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _error_message(error: Exception) -> str:
    message = getattr(error, "message", None)
    if isinstance(message, str) and message:
        return message
    return str(error) or type(error).__name__


def _platform_context(
    error: Exception,
    *,
    code: int | None = None,
    http_status: int | None = None,
    message: str,
) -> dict[str, int | str]:
    context: dict[str, int | str] = {
        "exception": type(error).__name__,
        "message": message,
    }
    if code is not None:
        context["code"] = code
    if http_status is not None:
        context["http_status"] = http_status
    return context


def _map_rejection(code: int, message: str) -> UteError:
    lowered_message = message.lower()
    if "insufficient balance" in lowered_message:
        return InsufficientBalanceError(message)
    if "market is closed" in lowered_message:
        return MarketClosedError(message)
    if "unknown order" in lowered_message:
        return OrderNotFoundError(message)
    if "duplicate order" in lowered_message:
        return DuplicateOrderIdError(message)
    return InvalidOrderError(message or f"Binance order rejection {code}")


def _map_server_busy(message: str) -> UteError:
    if "throttl" in message.lower():
        return RateLimitError(message)
    return PlatformConnectionError(message)


def map_binance_error(
    error: Exception,
    *,
    product: BinanceProduct | None = None,
) -> UteError:
    """Convert a python-binance or transport exception to a core error.

    Unknown Binance responses retain their code, HTTP status, message, and
    SDK exception type in ``PlatformError.platform_error``. Request URLs and
    response objects are excluded to avoid retaining signed data. Call this
    only for exceptions raised by Binance I/O; programming errors should not
    be translated at a broader adapter boundary. Supply ``product`` where
    known because some Binance codes have different meanings across products.
    """
    if isinstance(error, TimeoutError):
        return PlatformConnectionError("Binance request timed out")
    if isinstance(error, (ClientError, OSError)):
        status = _as_int(getattr(error, "status", None))
        status_text = f", HTTP {status}" if status is not None else ""
        return PlatformConnectionError(
            f"Binance transport failure ({type(error).__name__}{status_text})"
        )

    message = _error_message(error)

    if isinstance(error, (BinanceAPIException, BinanceOrderException)):
        code = _as_int(getattr(error, "code", None))
        http_status = _as_int(getattr(error, "status_code", None))

        if http_status in (418, 429):
            return RateLimitError(message)
        if http_status in (401, 403):
            return PlatformError(
                message,
                platform_error=_platform_context(
                    error,
                    code=code,
                    http_status=http_status,
                    message=message,
                ),
            )
        if code == -1008:
            return _map_server_busy(message)
        if code == -2026:
            if product in (BinanceProduct.USD_M, BinanceProduct.COIN_M):
                return InvalidOrderError(message)
            return PlatformError(
                message,
                platform_error=_platform_context(
                    error,
                    code=code,
                    http_status=http_status,
                    message=message,
                ),
            )
        if http_status is not None and 500 <= http_status < 600:
            return PlatformConnectionError(message)

        if code is not None:
            error_type = _BINANCE_CODE_MAP.get(code)
            if error_type is not None:
                if code in (-1010, -2010, -2011, -2038):
                    return _map_rejection(code, message)
                if error_type is PlatformError:
                    return PlatformError(
                        message or f"Binance error {code}",
                        platform_error=_platform_context(
                            error,
                            code=code,
                            http_status=http_status,
                            message=message or f"Binance error {code}",
                        ),
                    )
                return error_type(message or f"Binance error {code}")

        return PlatformError(
            message or "Unknown Binance API error",
            platform_error=_platform_context(
                error,
                code=code,
                http_status=http_status,
                message=message or "Unknown Binance API error",
            ),
        )

    if isinstance(error, BinanceRequestException):
        return PlatformError(
            message,
            platform_error=_platform_context(error, message=message),
        )

    safe_message = f"Unexpected Binance error ({type(error).__name__})"
    return PlatformError(
        safe_message,
        platform_error={"exception": type(error).__name__},
    )
