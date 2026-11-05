"""Immutable configuration for the Binance adapter scaffold."""

from __future__ import annotations

from dataclasses import dataclass, field


def _validate_non_empty_string(name: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    if value != value.strip():
        raise ValueError(f"{name} must not contain surrounding whitespace")


@dataclass(frozen=True, slots=True)
class BinanceConfig:
    """Binance credentials and environment selection.

    ``testnet`` selects Binance Testnet endpoints and ``demo`` selects Binance
    Demo Trading endpoints. They are mutually exclusive. When both are false,
    the selected environment is production (live). Both default to false to
    match the SDK's live-by-default behavior. Exchange connectivity is not
    implemented by this scaffold.
    """

    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)
    testnet: bool = False
    demo: bool = False
    account_id: str | None = None

    def __post_init__(self) -> None:
        _validate_non_empty_string("api_key", self.api_key)
        _validate_non_empty_string("api_secret", self.api_secret)

        if type(self.testnet) is not bool:
            raise TypeError("testnet must be a bool")
        if type(self.demo) is not bool:
            raise TypeError("demo must be a bool")
        if self.testnet and self.demo:
            raise ValueError("testnet and demo are mutually exclusive")

        if self.account_id is not None:
            _validate_non_empty_string("account_id", self.account_id)
