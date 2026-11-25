# Namespace package (pkgutil-style) — enables multiple independently-installable
# packages to contribute modules under a single unified_trading_execution namespace.
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)

# Every distribution ships this file, so the copy pip leaves in place must
# restore the core public surface.  Adapter copies run the same two imports.
from unified_trading_execution._public import *  # noqa: F403
from unified_trading_execution._public import __all__ as __all__
