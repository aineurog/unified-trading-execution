"""Binance market product identifiers."""

from __future__ import annotations

from enum import StrEnum


class BinanceProduct(StrEnum):
    """Market-data and trading API family for a Binance instrument."""

    SPOT = "spot"
    USD_M = "usd_m"
    COIN_M = "coin_m"
