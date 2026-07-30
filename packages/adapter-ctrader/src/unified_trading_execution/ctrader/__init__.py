# Namespace package (pkgutil-style) — extends unified_trading_execution namespace.
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

from unified_trading_execution.ctrader.adapter import CTraderAdapter  # noqa: F401
