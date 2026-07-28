# Namespace package (pkgutil-style) — enables the bybit adapter to contribute
# modules under the unified_trading_execution namespace alongside the core package.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)
