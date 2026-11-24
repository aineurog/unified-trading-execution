"""Guards the shared namespace init against install order regressions."""

from __future__ import annotations

import unified_trading_execution as package
from unified_trading_execution import _public


def test_package_reexports_the_public_surface() -> None:
    assert package.__all__ == _public.__all__
    missing = [name for name in _public.__all__ if not hasattr(package, name)]
    assert missing == []
