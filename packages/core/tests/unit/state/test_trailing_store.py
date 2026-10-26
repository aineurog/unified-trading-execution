"""Unit tests for trailing-state persistence."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio

from unified_trading_execution.state.store import SQLiteStateStore
from unified_trading_execution.trailing.types import (
    CandleTimeframe,
    ProtectionLevel,
    ProtectionLevelType,
    TrailingActivation,
    TrailingBasis,
    TrailingMode,
    TrailingOffsetType,
    TrailingPhase,
    TrailingSpec,
    TrailingState,
)
from unified_trading_execution.types.enums import AssetClass, OrderSide
from unified_trading_execution.types.instrument import Instrument
from unified_trading_execution.types.position import Position

NOW = datetime(2026, 7, 28, 12, 0, 0, tzinfo=UTC)


def make_inst(symbol="BTC", quote_currency="USDT"):
    return Instrument(
        symbol=symbol,
        quote_currency=quote_currency,
        asset_class=AssetClass.SPOT,
        exchange=None,
        currency=None,
        expiry=None,
        strike=None,
        option_right=None,
        multiplier=None,
    )


def make_state(
    trailing_id="tr-1",
    *,
    instrument=None,
    position_id="1",
    phase=TrailingPhase.ARMED,
    **overrides,
):
    base = TrailingState(
        trailing_id=trailing_id,
        instrument=instrument or make_inst(),
        position_id=position_id,
        mode=TrailingMode.SYNTHETIC,
        phase=phase,
        spec=TrailingSpec(
            offset_type=TrailingOffsetType.PRICE,
            offset=Decimal("100"),
        ),
        side=OrderSide.BUY,
        entry_price=Decimal("50000"),
        quantity=Decimal("0.5"),
        created_at=NOW,
        updated_at=NOW,
    )
    return replace(base, **overrides) if overrides else base


@pytest_asyncio.fixture
async def store():
    s = SQLiteStateStore(":memory:")
    await s.initialize()
    yield s
    await s.close()


class TestTrailingStateStore:
    async def test_full_round_trip(self, store):
        state = TrailingState(
            trailing_id="tr-1",
            instrument=make_inst(),
            position_id="leg-1",
            mode=TrailingMode.SYNTHETIC,
            phase=TrailingPhase.ARMED,
            spec=TrailingSpec(
                offset_type=TrailingOffsetType.ATR,
                offset=Decimal("2.50"),
                basis=TrailingBasis.LAST,
                activation=TrailingActivation.PRICE,
                activation_price=Decimal("51000"),
                initial_stop=ProtectionLevel(ProtectionLevelType.PRICE, Decimal("49000")),
                take_profit=ProtectionLevel(ProtectionLevelType.ATR, Decimal("4")),
                step=Decimal("10"),
                atr_period=14,
                atr_timeframe=CandleTimeframe.H1,
            ),
            side=OrderSide.SELL,
            entry_price=Decimal("50000.10"),
            quantity=Decimal("0.50"),
            created_at=NOW,
            updated_at=NOW + timedelta(minutes=1),
            position_opened_at=NOW - timedelta(hours=1),
            routing_reason="synthetic_only",
            resolved_basis=TrailingBasis.MARK,
            protection_trigger_basis=TrailingBasis.LAST,
            current_stop=Decimal("49500"),
            current_take_profit=Decimal("55000"),
            stop_owned=True,
            take_profit_owned=True,
            previous_stop_loss=Decimal("49000"),
            previous_take_profit=Decimal("54000"),
            best_basis=Decimal("51200"),
            resolved_activation_price=Decimal("51000"),
            activated=True,
            atr_value=Decimal("250.125"),
            atr_as_of=NOW,
            native_id="native-1",
            last_reprice_at=NOW,
            reprice_count=3,
            last_error="boom",
            failure_count=2,
        )
        await store.upsert_trailing_state(state)

        loaded = await store.get_trailing_state("tr-1")
        assert loaded == state

    async def test_missing_state_returns_none(self, store):
        assert await store.get_trailing_state("nope") is None

    async def test_active_uniqueness_blocks_second_attach(self, store):
        await store.upsert_trailing_state(make_state("tr-1"))
        with pytest.raises(sqlite3.IntegrityError):
            await store.upsert_trailing_state(make_state("tr-2"))

        assert await store.get_trailing_state("tr-2") is None
        assert len(await store.list_trailing_states()) == 1

    async def test_terminal_state_does_not_block_new_attach(self, store):
        await store.upsert_trailing_state(make_state("tr-1", phase=TrailingPhase.DONE))
        await store.upsert_trailing_state(make_state("tr-2"))

        states = await store.list_trailing_states()
        assert {state.trailing_id for state in states} == {"tr-1", "tr-2"}
        assert await store.get_trailing_state("tr-1") is not None

    async def test_instrument_filter_uses_quote_currency(self, store):
        usdt = make_inst(symbol="BTC", quote_currency="USDT")
        usd = make_inst(symbol="BTC", quote_currency="USD")
        await store.upsert_trailing_state(make_state("tr-usdt", instrument=usdt))
        await store.upsert_trailing_state(make_state("tr-usd", instrument=usd))

        assert [s.trailing_id for s in await store.list_trailing_states(instrument=usdt)] == [
            "tr-usdt"
        ]
        assert [s.trailing_id for s in await store.list_trailing_states(instrument=usd)] == [
            "tr-usd"
        ]

    async def test_phase_filter(self, store):
        await store.upsert_trailing_state(make_state("tr-armed", phase=TrailingPhase.ARMED))
        await store.upsert_trailing_state(make_state("tr-done", phase=TrailingPhase.DONE))

        armed = await store.list_trailing_states(phases=(TrailingPhase.ARMED,))
        assert [s.trailing_id for s in armed] == ["tr-armed"]

        assert await store.list_trailing_states(phases=()) == []


class TestPositionOpenedAt:
    async def test_opened_at_round_trips(self, store):
        position = Position(
            instrument=make_inst(),
            quantity=Decimal("0.5"),
            average_entry_price=Decimal("50000"),
            updated_at=NOW,
            position_id="1",
            opened_at=NOW - timedelta(minutes=5),
        )
        await store.upsert_position(position)

        legs = await store.get_positions(position.instrument)
        assert len(legs) == 1
        assert legs[0].opened_at == position.opened_at

        queried = await store.query_positions()
        assert len(queried) == 1
        assert queried[0].opened_at == position.opened_at

    async def test_opened_at_none_round_trips(self, store):
        position = Position(
            instrument=make_inst(),
            quantity=Decimal("0.5"),
            average_entry_price=Decimal("50000"),
            updated_at=NOW,
            position_id="1",
        )
        await store.upsert_position(position)

        legs = await store.get_positions(position.instrument)
        assert legs[0].opened_at is None
