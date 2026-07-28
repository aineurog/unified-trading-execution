"""Canonical Instrument <-> Bybit symbol translation goes here.

The engine uses ``Instrument`` (symbol, quote_currency, asset_class, ...)
everywhere.  Bybit uses strings like "BTCUSDT" for spot and "BTCUSDT.P"
for perpetuals.  This module must provide bidirectional translation so
the adapter can convert back and forth without leaking Bybit naming
conventions into core.

TODO: implement:
  - to_bybit_symbol(instrument: Instrument) -> str
  - from_bybit_symbol(bybit_symbol: str, asset_class: AssetClass) -> Instrument
"""

from __future__ import annotations
