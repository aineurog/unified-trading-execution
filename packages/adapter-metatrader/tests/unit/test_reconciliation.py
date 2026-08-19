"""Unit tests for the MT5 reconciliation read API.

Tests run against the mocked ``MetaTrader5`` module — no real terminal IPC.

Tests cases:
    - fetch_positions: nets hedging legs per instrument, skips unknown symbols
    - fetch_positions: ``positions_get()`` failure raises PlatformError
    - fetch_balances: free is derived as total - used (invariant preserved)
    - get_rate_limits: returns the conservative fixed estimate
    - fetch_open_orders: builds OrderRecords for LIMIT/STOP/STOP_LIMIT
    - fetch_open_orders: unknown ticket keys by platform id, bad symbols skipped
    - fetch_open_orders: unknown type/state raises PlatformError
    - fetch_fills: groups trading deals by client_order_id, excludes non-trading
    - fetch_fills: never advances the poll baseline
    - _resolve_mt5_symbol: alias wins, override fallback, non-pair guard
"""

from __future__ import annotations

from collections import namedtuple
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from unified_trading_execution.errors import PlatformError
from unified_trading_execution.mt5 import MT5Adapter
from unified_trading_execution.mt5.adapter import _DEAL_QUERY_BACKLOG_SECONDS
from unified_trading_execution.types.enums import (
    AssetClass,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from unified_trading_execution.types.instrument import Instrument, _with_broker_override

_PAST = datetime(2024, 1, 1, tzinfo=UTC)
_DEAL_TIME = int(datetime(2024, 1, 2, 12, 0, 0, tzinfo=UTC).timestamp())

Mt5Order = namedtuple(
    "Mt5Order",
    [
        "ticket",
        "time_setup",
        "time_done",
        "type",
        "state",
        "volume_initial",
        "volume_current",
        "price_open",
        "price_stoplimit",
        "sl",
        "tp",
        "symbol",
        "type_time",
        "time_expiration",
        "position_id",
    ],
)
Mt5Position = namedtuple(
    "Mt5Position",
    ["ticket", "time", "time_update", "type", "symbol", "volume", "price_open"],
)
Mt5Deal = namedtuple(
    "Mt5Deal",
    [
        "ticket",
        "order",
        "time",
        "type",
        "entry",
        "symbol",
        "volume",
        "price",
        "commission",
        "fee",
        "position_id",
    ],
)


def _order(**overrides: object) -> Mt5Order:
    """A MagicMock-free order tuple with realistic pending-limit defaults."""
    base: dict[str, object] = {
        "ticket": 1001,
        "time_setup": int(_PAST.timestamp()),
        "time_done": 0,
        "type": 2,  # ORDER_TYPE_BUY_LIMIT
        "state": 1,  # ORDER_STATE_PLACED
        "volume_initial": 0.1,
        "volume_current": 0.1,
        "price_open": 1.1000,
        "price_stoplimit": 0.0,
        "sl": 0.0,
        "tp": 1.1100,
        "symbol": "EURUSD.m",
        "type_time": 0,  # ORDER_TIME_GTC
        "time_expiration": 0,
        "position_id": 0,
    }
    base.update(overrides)
    return Mt5Order(**base)


def _account(**overrides: object) -> MagicMock:
    """An ``account_info()`` snapshot with realistic monetary fields."""
    base: dict[str, object] = {
        "login": 12345678,
        "currency": "USD",
        "balance": 1000.0,
        "equity": 1000.0,
        "margin": 0.0,
        "margin_free": 1000.0,
    }
    base.update(overrides)
    return MagicMock(**base)


def _set_eurusd_symbol_info(mock_mt5_module: MagicMock) -> None:
    """Configure ``symbol_info()`` to resolve ``EURUSD.m`` as a forex pair."""
    mock_mt5_module.symbol_info.return_value = MagicMock(path="Forex\\EURUSD")


def _prepared(adapter: MT5Adapter) -> MT5Adapter:
    """Wire the reverse alias table so inbound symbols resolve."""
    adapter._build_reverse_alias()
    return adapter


class TestFetchPositions:
    """fetch_positions — netting, symbol resolution, failure paths."""

    async def test_nets_hedging_legs_per_instrument(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """BUY and SELL legs on one symbol collapse into a signed Position."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        mock_mt5_module.positions_get.return_value = (
            Mt5Position(
                ticket=2001,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="EURUSD.m",
                volume=0.2,
                price_open=1.1000,
            ),
            Mt5Position(
                ticket=2002,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=1,
                symbol="EURUSD.m",
                volume=0.1,
                price_open=1.1000,
            ),
        )

        positions = await adapter.fetch_positions()

        assert len(positions) == 1
        position = positions[next(iter(positions))]
        assert position.quantity == Decimal("0.1")
        assert position.average_entry_price == Decimal("1.1")

    async def test_unresolvable_symbol_skipped(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """A symbol with no path mapping is skipped, not fatal."""
        _prepared(adapter)

        def fake_symbol_info(symbol: str) -> MagicMock:
            if symbol == "EURUSD.m":
                return MagicMock(path="Forex\\EURUSD")
            return MagicMock()

        mock_mt5_module.symbol_info.side_effect = fake_symbol_info
        mock_mt5_module.positions_get.return_value = (
            Mt5Position(
                ticket=2001,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="EURUSD.m",
                volume=0.1,
                price_open=1.1000,
            ),
            Mt5Position(
                ticket=2002,
                time=_DEAL_TIME,
                time_update=_DEAL_TIME,
                type=0,
                symbol="BADSYM.m",
                volume=0.1,
                price_open=1.1000,
            ),
        )

        positions = await adapter.fetch_positions()

        assert len(positions) == 1

    async def test_positions_get_none_raises_platform_error(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """``positions_get()`` returning None maps to a PlatformError."""
        mock_mt5_module.positions_get.return_value = None
        mock_mt5_module.last_error.return_value = (10011, "unknown symbol")

        with pytest.raises(PlatformError):
            await adapter.fetch_positions()


class TestFetchBalances:
    """fetch_balances — mapping and invariant preservation."""

    async def test_free_derived_as_total_minus_used(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """free is recomputed so free + used == total holds exactly."""
        mock_mt5_module.account_info.return_value = _account(
            equity=1000.0,
            margin=123.45,
        )

        balances = await adapter.fetch_balances()

        balance = balances["USD"]
        assert balance.total == Decimal("1000")
        assert balance.used == Decimal("123.45")
        assert balance.free == Decimal("876.55")
        assert balance.free + balance.used == balance.total

    async def test_account_info_none_raises_platform_error(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """``account_info()`` returning None maps to a PlatformError."""
        mock_mt5_module.account_info.return_value = None
        mock_mt5_module.last_error.return_value = (10011, "not initialized")

        with pytest.raises(PlatformError):
            await adapter.fetch_balances()


class TestGetRateLimits:
    """get_rate_limits — the conservative fixed estimate."""

    async def test_returns_conservative_estimate(self, adapter: MT5Adapter) -> None:
        """MT5 has no rate-limit endpoint; a fixed 1 req/sec is reported."""
        limits = await adapter.get_rate_limits()

        assert limits.requests_per_interval == 1
        assert limits.interval_seconds == 1.0
        assert limits.remaining == 1
        assert isinstance(limits.reset_at, datetime)


class TestFetchOpenOrders:
    """fetch_open_orders — OrderRecord reconstruction, keys, failures."""

    async def test_builds_limit_order_record(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """A pending LIMIT BUY maps to a full OrderRecord."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        adapter._ticket_to_order_id = {1001: "client-abc"}
        mock_mt5_module.orders_get.return_value = (_order(),)

        records = await adapter.fetch_open_orders()

        record = records["client-abc"]
        assert record.instrument.symbol == "EUR"
        assert record.order_type is OrderType.LIMIT
        assert record.side is OrderSide.BUY
        assert record.quantity == Decimal("0.1")
        assert record.time_in_force is TimeInForce.GTC
        assert record.price == Decimal("1.1")
        assert record.stop_price is None
        assert record.take_profit is not None
        assert record.take_profit.trigger_price == Decimal("1.11")
        assert record.stop_loss is None
        assert record.platform_order_id == "1001"
        assert record.status is OrderStatus.OPEN
        assert record.filled_quantity == Decimal("0")

    async def test_stop_order_price_mapping(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """STOP keeps its trigger in ``stop_price`` (price_open)."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        mock_mt5_module.orders_get.return_value = (_order(type=5),)  # SELL_STOP

        record = next(iter((await adapter.fetch_open_orders()).values()))

        assert record.order_type is OrderType.STOP
        assert record.side is OrderSide.SELL
        assert record.price is None
        assert record.stop_price == Decimal("1.1")

    async def test_stop_limit_price_mapping(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """STOP_LIMIT keeps trigger in stop_price and limit in price."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        mock_mt5_module.orders_get.return_value = (
            _order(type=6, price_open=1.1500, price_stoplimit=1.1600),
        )  # BUY_STOP_LIMIT

        record = next(iter((await adapter.fetch_open_orders()).values()))

        assert record.order_type is OrderType.STOP_LIMIT
        assert record.price == Decimal("1.16")
        assert record.stop_price == Decimal("1.15")

    async def test_unknown_ticket_keys_by_platform_id(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """An order placed outside the engine keys by platform id."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        mock_mt5_module.orders_get.return_value = (_order(),)

        records = await adapter.fetch_open_orders()

        assert "1001" in records
        assert records["1001"].client_order_id == ""

    async def test_unresolvable_symbol_skipped(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """An order for an unknown symbol is skipped with a warning."""
        _prepared(adapter)

        def fake_symbol_info(symbol: str) -> MagicMock:
            if symbol == "EURUSD.m":
                return MagicMock(path="Forex\\EURUSD")
            return MagicMock()

        mock_mt5_module.symbol_info.side_effect = fake_symbol_info
        mock_mt5_module.orders_get.return_value = (
            _order(),
            _order(ticket=1002, symbol="BADSYM.m"),
        )

        records = await adapter.fetch_open_orders()

        assert len(records) == 1

    async def test_unknown_order_type_raises_platform_error(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """An unrecognized MT5 order type is a PlatformError."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        mock_mt5_module.orders_get.return_value = (_order(type=99),)

        with pytest.raises(PlatformError, match="order type 99"):
            await adapter.fetch_open_orders()

    async def test_unknown_state_raises_platform_error(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """An unrecognized MT5 order state is a PlatformError."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        mock_mt5_module.orders_get.return_value = (_order(state=99),)

        with pytest.raises(PlatformError, match="order state 99"):
            await adapter.fetch_open_orders()

    async def test_transient_states_map_to_open(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """STARTED and REQUEST_* states (0, 7-9) are transient but valid in
        orders_get() — they must map to OPEN, never abort the fetch."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        mock_mt5_module.orders_get.return_value = (
            _order(ticket=1001, state=0),  # ORDER_STATE_STARTED
            _order(ticket=1002, state=7),  # ORDER_STATE_REQUEST_ADD
            _order(ticket=1003, state=8),  # ORDER_STATE_REQUEST_MODIFY
            _order(ticket=1004, state=9),  # ORDER_STATE_REQUEST_CANCEL
        )

        records = await adapter.fetch_open_orders()

        assert {record.status for record in records.values()} == {OrderStatus.OPEN}

    async def test_orders_get_none_raises_platform_error(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """``orders_get()`` returning None maps to a PlatformError."""
        mock_mt5_module.orders_get.return_value = None
        mock_mt5_module.last_error.return_value = (10011, "unknown symbol")

        with pytest.raises(PlatformError):
            await adapter.fetch_open_orders()


class TestFetchFills:
    """fetch_fills — trading-deal filtering, grouping, baseline preservation."""

    def _deal(self, **overrides: object) -> Mt5Deal:
        base: dict[str, object] = {
            "ticket": 3001,
            "order": 1001,
            "time": _DEAL_TIME,
            "type": 0,  # DEAL_TYPE_BUY
            "entry": 0,
            "symbol": "EURUSD.m",
            "volume": 0.1,
            "price": 1.1000,
            "commission": 0.0,
            "fee": 0.0,
            "position_id": 0,
        }
        base.update(overrides)
        return Mt5Deal(**base)

    async def test_groups_trading_deals_by_client_order_id(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """BUY/SELL deals map to FillRecords keyed by client_order_id."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        adapter._ticket_to_order_id = {1001: "client-abc"}
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = (
            self._deal(),
            self._deal(ticket=3002, type=2),  # DEAL_TYPE_IN — not a fill
        )

        fills = await adapter.fetch_fills()

        assert list(fills) == ["client-abc"]
        fill = fills["client-abc"][0]
        assert fill.client_order_id == "client-abc"
        assert fill.platform_fill_id == "3001"
        assert fill.instrument.symbol == "EUR"
        assert fill.fill_quantity == Decimal("0.1")
        assert fill.fill_price == Decimal("1.1")
        assert fill.fee_amount is None

    async def test_does_not_advance_poll_baseline(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """Reconciliation must not disturb the poll loop's dedup state."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        adapter._ticket_to_order_id = {1001: "client-abc"}
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = (self._deal(),)
        last_time = adapter._last_deal_time
        last_ticket = adapter._last_deal_ticket

        await adapter.fetch_fills()

        assert adapter._last_deal_time == last_time
        assert adapter._last_deal_ticket == last_ticket

    async def test_unresolvable_symbol_deal_skipped(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """Deals for unresolvable symbols are skipped, not fatal."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        adapter._ticket_to_order_id = {1001: "client-abc"}
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = (self._deal(symbol="BADSYM.m"),)

        fills = await adapter.fetch_fills()

        assert fills == {}

    async def test_non_positive_volume_or_price_deal_skipped(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """A zero/negative-volume or -price deal must not violate FillRecord's
        fill_quantity/fill_price > 0 invariant — skip it like the poll loop."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        adapter._ticket_to_order_id = {1001: "client-abc"}
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = (
            self._deal(volume=0.0),
            self._deal(ticket=3002, price=0.0),
        )

        fills = await adapter.fetch_fills()

        assert fills == {}

    async def test_history_deals_get_none_raises_platform_error(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """``history_deals_get()`` returning None maps to a PlatformError."""
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = None
        mock_mt5_module.last_error.return_value = (10011, "not initialized")

        with pytest.raises(PlatformError):
            await adapter.fetch_fills()

    async def test_server_offset_deal_reported_with_real_utc_timestamp(
        self, mock_mt5_module: MagicMock, adapter: MT5Adapter
    ) -> None:
        """fetch_fills shifts its query into the server-as-epoch basis and
        reports server-as-epoch deals with real-UTC timestamps."""
        _prepared(adapter)
        _set_eurusd_symbol_info(mock_mt5_module)
        offset = 10800
        adapter._server_time_offset = offset
        adapter._ticket_to_order_id = {1001: "client-abc"}
        mock_mt5_module.account_info.return_value = _account()
        mock_mt5_module.history_deals_get.return_value = (self._deal(time=_DEAL_TIME + offset),)

        fills = await adapter.fetch_fills()

        fill = fills["client-abc"][0]
        assert fill.fill_timestamp == datetime.fromtimestamp(_DEAL_TIME, tz=UTC)
        call_args = mock_mt5_module.history_deals_get.call_args.args
        assert call_args[0] == (
            int(adapter._last_deal_time.timestamp()) + offset - _DEAL_QUERY_BACKLOG_SECONDS
        )


class TestResolveMt5Symbol:
    """_resolve_mt5_symbol — alias precedence, override fallback, non-pair guard."""

    def _instrument(self, symbol: str = "EUR", quote: str | None = "USD") -> Instrument:
        return Instrument(
            symbol=symbol,
            quote_currency=quote,
            asset_class=AssetClass.MARGIN_FX,
        )

    def test_alias_wins_over_override(self, adapter: MT5Adapter) -> None:
        """The alias table beats a pre-set broker_symbol_override."""
        inst = _with_broker_override(self._instrument(), "EURUSDpro")

        assert adapter._resolve_mt5_symbol(inst) == "EURUSD.m"

    def test_broker_override_used_without_alias(self, adapter: MT5Adapter) -> None:
        """Without an alias, the instrument's own override is honoured."""
        inst = _with_broker_override(self._instrument(symbol="GBP"), "GBPUSDpro")

        assert adapter._resolve_mt5_symbol(inst) == "GBPUSDpro"

    def test_non_pair_stock_uses_override(self, adapter: MT5Adapter) -> None:
        """``str()`` raises for STOCK — the override still resolves."""
        inst = _with_broker_override(
            Instrument(symbol="AAPL", asset_class=AssetClass.STOCK), "AAPL.US"
        )

        assert adapter._resolve_mt5_symbol(inst) == "AAPL.US"

    def test_non_pair_without_override_raises(self, adapter: MT5Adapter) -> None:
        """No quote and no override means no usable MT5 symbol."""
        inst = Instrument(symbol="AAPL", asset_class=AssetClass.STOCK)

        with pytest.raises(ValueError):
            adapter._resolve_mt5_symbol(inst)
