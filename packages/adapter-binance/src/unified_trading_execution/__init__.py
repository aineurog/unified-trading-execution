"""Extend the core package namespace with modules from the Binance adapter."""

from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

# Every distribution ships this file, so the copy pip leaves in place must
# restore the core public surface.  The core copy runs the same two imports.
from unified_trading_execution._public import *  # noqa: F403
from unified_trading_execution._public import __all__ as __all__
