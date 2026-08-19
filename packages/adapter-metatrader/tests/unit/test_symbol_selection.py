"""Unit tests for Market Watch symbol selection (``_ensure_symbol_selected``).

MT5 streams real-time quotes only for symbols selected in Market Watch;
``symbol_info_tick()`` returns ``None`` and ``order_send()`` can reject
unselected symbols.  These tests cover the selection helper's idempotence,
caching, and error mapping — no real terminal IPC.

Tests cases:
    - selects a symbol not yet selected, then caches it
    - a cached selection is not re-issued on later calls
    - a symbol the broker does not provide raises InvalidSymbolError and is
      remembered so it is not retried every call
    - a transient selection failure raises without caching the symbol, so a
      later call retries
    - a previously-failed symbol is skipped without calling symbol_select
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from unified_trading_execution.errors import InvalidSymbolError, PlatformError
from unified_trading_execution.mt5 import MT5Adapter


class TestEnsureSymbolSelected:
    """_ensure_symbol_selected — selection, caching, error paths."""

    def test_selects_and_caches(self, adapter: MT5Adapter, mock_mt5_module: MagicMock) -> None:
        """A symbol not yet selected is added to Market Watch and cached."""
        adapter._ensure_symbol_selected("EURUSD.m", mock_mt5_module)

        mock_mt5_module.symbol_select.assert_called_once_with("EURUSD.m", True)
        assert adapter._selected_symbols == {"EURUSD.m"}

    def test_cached_selection_not_reissued(
        self, adapter: MT5Adapter, mock_mt5_module: MagicMock
    ) -> None:
        """Already-selected symbols are skipped — no repeated IPC call."""
        adapter._ensure_symbol_selected("EURUSD.m", mock_mt5_module)
        adapter._ensure_symbol_selected("EURUSD.m", mock_mt5_module)

        mock_mt5_module.symbol_select.assert_called_once_with("EURUSD.m", True)

    def test_unknown_symbol_raises_and_is_remembered(
        self, adapter: MT5Adapter, mock_mt5_module: MagicMock
    ) -> None:
        """A symbol the broker does not provide → InvalidSymbolError + cache."""
        mock_mt5_module.symbol_select.return_value = False
        mock_mt5_module.last_error.return_value = (4301, "unknown symbol")

        with pytest.raises(InvalidSymbolError, match="unknown symbol"):
            adapter._ensure_symbol_selected("EURUSD.m", mock_mt5_module)

        assert "EURUSD.m" in adapter._failed_symbols

    def test_failed_symbol_not_retried(
        self, adapter: MT5Adapter, mock_mt5_module: MagicMock
    ) -> None:
        """A broker-missing symbol is not re-issued on every call."""
        mock_mt5_module.symbol_select.return_value = False
        mock_mt5_module.last_error.return_value = (4301, "unknown symbol")
        with pytest.raises(InvalidSymbolError):
            adapter._ensure_symbol_selected("EURUSD.m", mock_mt5_module)

        adapter._ensure_symbol_selected("EURUSD.m", mock_mt5_module)

        mock_mt5_module.symbol_select.assert_called_once_with("EURUSD.m", True)

    def test_transient_failure_raises_without_caching(
        self, adapter: MT5Adapter, mock_mt5_module: MagicMock
    ) -> None:
        """A transient failure raises but is not cached — a later call retries."""
        mock_mt5_module.symbol_select.return_value = False
        mock_mt5_module.last_error.return_value = (4302, "not selected")

        with pytest.raises(PlatformError):
            adapter._ensure_symbol_selected("EURUSD.m", mock_mt5_module)

        assert "EURUSD.m" not in adapter._failed_symbols

        mock_mt5_module.symbol_select.return_value = True
        adapter._ensure_symbol_selected("EURUSD.m", mock_mt5_module)
        assert "EURUSD.m" in adapter._selected_symbols

    def test_previously_failed_symbol_skipped(
        self, adapter: MT5Adapter, mock_mt5_module: MagicMock
    ) -> None:
        """Symbols in ``_failed_symbols`` are skipped without an IPC call."""
        adapter._failed_symbols.add("FAKEUSD")

        adapter._ensure_symbol_selected("FAKEUSD", mock_mt5_module)

        mock_mt5_module.symbol_select.assert_not_called()
