"""Binance market product identifiers and Futures configuration enums."""

from __future__ import annotations

from enum import StrEnum


class BinanceProduct(StrEnum):
    """Market-data and trading API family for a Binance instrument."""

    SPOT = "spot"
    USD_M = "usd_m"
    COIN_M = "coin_m"


class MarginMode(StrEnum):
    """Binance Futures margin mode for one product symbol."""

    CROSS = "cross"
    ISOLATED = "isolated"


class PositionMode(StrEnum):
    """Binance Futures account position mode."""

    ONE_WAY = "one_way"
    HEDGE = "hedge"
