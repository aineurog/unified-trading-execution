"""Async Engine — the central orchestrator users interact with.

The Engine owns the lifecycle: it wires together the adapter, risk-check
chain, state mirror, event bus, halt state machine, and audit trail. Users
call methods on the Engine, not directly on the adapter — the Engine runs
risk checks, generates IDs, enforces halt rules, tracks rate limits, and
delegates translation-only work to the adapter.

Architecture:
    Engine (lifecycle + public API)
      └─ dispatch/ (pure async orchestration functions)
           ├─ dispatch_place_order
           ├─ dispatch_modify_order
           └─ dispatch_cancel_order
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter
from unified_trading_execution.dispatch import (
    dispatch_cancel_order,
    dispatch_modify_order,
    dispatch_place_order,
)
from unified_trading_execution.errors import EngineShutdownError
from unified_trading_execution.events import (
    AuditEvent,
    BalanceUpdateEvent,
    EventBus,
    FillEvent,
    HaltClearedEvent,
    HaltEnteredEvent,
    HaltEvent,
    PositionUpdateEvent,
    ReconciliationCompleteEvent,
    ReconciliationEvent,
)
from unified_trading_execution.risk import RiskConfig
from unified_trading_execution.state import (
    HaltConfig,
    HaltStateMachine,
    ReconciliationResult,
    StateStore,
    reconcile,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

logger = logging.getLogger(__name__)


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


class Engine:
    """Async-native trading engine — the main entry point.

    Construction::

        engine = Engine(
            adapter=BybitAdapter(...),
            state_store=SQLiteStateStore("path/to/db"),
            get_reference_price=my_price_fn,  # optional
            event_bus=EventBus(),             # optional (auto-created)
            risk_config=RiskConfig(...),      # optional (sensible defaults)
            halt_config=HaltConfig(...),      # optional (auto-halt enabled)
        )
        await engine.connect()

    Usage::

        order = UnifiedOrder(...)
        result = await engine.place_order(order)
        await engine.disconnect()
    """

    def __init__(
        self,
        adapter: Adapter,
        state_store: StateStore,
        *,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
    ) -> None:
        self._adapter = adapter
        self._state_store = state_store
        self._get_reference_price = get_reference_price
        self._event_bus = event_bus or EventBus()
        self._risk_config = risk_config or RiskConfig()
        self._halt_machine = HaltStateMachine(halt_config)
        self._shutdown = False
        self._adapter.attach_halt_machine(self._halt_machine)
        self._adapter.attach_event_bus(self._event_bus)

        # Mutable cached state
        self._instrument_specs: dict[Instrument, InstrumentSpec] = {}
        self._known_order_ids: set[str] = set()
        self._rate_limit_budget: int = 0
        self._rate_limit_reset_at: datetime | None = None
        self._rate_limit_refresh_lock = asyncio.Lock()

        # Wire up state-mirror subscriptions
        self._event_bus.subscribe(FillEvent, self._on_fill)
        self._event_bus.subscribe(PositionUpdateEvent, self._on_position_update)
        self._event_bus.subscribe(BalanceUpdateEvent, self._on_balance_update)

    # ── Lifecycle ──────────────────────────────────────────────────

    async def connect(self) -> None:
        """Connect the adapter, initialise the state store, seed caches."""
        await self._state_store.initialize()
        await self._adapter.connect()

        # Seed known order IDs from existing state
        try:
            existing = await self._state_store.query_orders(limit=100_000)
            self._known_order_ids = {o.client_order_id for o in existing}
        except Exception:
            logger.warning("Could not seed known_order_ids from state store")

        # Fetch initial rate limits
        await self._refresh_rate_limits()

    async def disconnect(self) -> None:
        """Disconnect the adapter gracefully."""
        await self._adapter.disconnect()

    async def ashutdown(self) -> None:
        """Ordered teardown: flush audit, disconnect adapter, close state store, mark dead."""
        if self._shutdown:
            return
        self._shutdown = True
        # Step 1 — flush pending audit writes to durable storage
        try:
            await self._state_store.flush()
        except Exception:
            logger.exception("Error during state store flush in shutdown")
        # Step 2 — disconnect adapter gracefully
        try:
            await self._adapter.disconnect()
        except Exception:
            logger.exception("Error during adapter disconnect in shutdown")
        # Step 3 — close state store
        await self._state_store.close()

    def shutdown(self) -> None:
        """Sync wrapper for ashutdown — convenience for sync users."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.ashutdown())
        else:
            future = asyncio.run_coroutine_threadsafe(self.ashutdown(), loop)
            future.result()

    # ── Order operations ───────────────────────────────────────────

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Place an order through the full pipeline.

        1. Fetch / cache instrument spec
        2. Resolve reference price
        3. Refresh rate-limit budget if stale
        4. Run risk-check chain (Section 7)
        5. Check halt state (Section 6.4)
        6. Generate IDs, delegate to adapter
        7. Persist OrderRecord, emit OrderPlacedEvent, write audit trail

        Idempotency (Section 9.2): if the adapter call times out, the engine
        queries the platform for the order status before allowing a retry.
        If the order exists on the platform, it is treated as a success.
        """
        self._check_not_shutdown()

        instrument_spec = await self._get_or_fetch_spec(order.instrument)
        reference_price = self._resolve_reference_price(order.instrument)
        await self._refresh_rate_limits_if_stale()

        # Capture the ID that will be used — dispatch may generate it
        client_order_id = order.client_order_id

        try:
            result = await dispatch_place_order(
                adapter=self._adapter,
                state_store=self._state_store,
                event_bus=self._event_bus,
                risk_config=self._risk_config,
                halt_machine=self._halt_machine,
                instrument_spec=instrument_spec,
                reference_price=reference_price,
                known_order_ids=frozenset(self._known_order_ids),
                rate_limit_budget=self._effective_budget(),
                order=order,
            )
        except TimeoutError:
            # Section 9.2: query platform before allowing retry
            cid = order.client_order_id or client_order_id
            if cid is not None:
                existing = await self._adapter.get_order_by_client_id(cid)
                if existing is not None:
                    logger.info(
                        "Timeout on place_order for %s — order exists on platform, "
                        "treating as success (status=%s)",
                        cid,
                        existing.status.value,
                    )
                    self._known_order_ids.add(existing.client_order_id)
                    return existing
            raise

        self._known_order_ids.add(result.client_order_id)
        self._rate_limit_budget -= 1
        return result

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify an existing order — risk-checked before dispatch."""
        self._check_not_shutdown()

        existing = await self._state_store.get_order(modification.client_order_id)
        if existing is None:
            from unified_trading_execution.errors import OrderNotFoundError

            raise OrderNotFoundError(modification.client_order_id)

        reference_price = self._resolve_reference_price(existing.instrument)
        await self._refresh_rate_limits_if_stale()

        # Exclude the order being modified from the duplicate check
        mod_known_ids = self._known_order_ids - {modification.client_order_id}

        result = await dispatch_modify_order(
            adapter=self._adapter,
            state_store=self._state_store,
            event_bus=self._event_bus,
            risk_config=self._risk_config,
            halt_machine=self._halt_machine,
            get_instrument_spec=self._get_or_fetch_spec,
            reference_price=reference_price,
            known_order_ids=frozenset(mod_known_ids),
            rate_limit_budget=self._effective_budget(),
            modification=modification,
        )

        self._rate_limit_budget -= 1
        return result

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an order by its client_order_id.

        Cancel is always permitted — no risk checks, no halt checks.
        """
        self._check_not_shutdown()

        result = await dispatch_cancel_order(
            adapter=self._adapter,
            state_store=self._state_store,
            event_bus=self._event_bus,
            client_order_id=client_order_id,
        )

        return result

    async def get_order(self, client_order_id: str) -> OrderResult | None:
        """Query an order's current status from the platform."""
        self._check_not_shutdown()
        return await self._adapter.get_order_by_client_id(client_order_id)

    # ── Instrument metadata ────────────────────────────────────────

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch and cache trading rules for an instrument."""
        self._check_not_shutdown()
        return await self._get_or_fetch_spec(instrument)

    # ── Reconciliation ─────────────────────────────────────────────

    async def reconcile(self) -> ReconciliationResult:
        """Run a full reconciliation pass: compare local mirror against platform.

        1. Gather local state from the state store
        2. Gather platform state from the adapter (if supported)
        3. Detect mismatches via the pure reconcile() function
        4. Apply resolution per case (Section 6.3)
        5. Publish ReconciliationCompleteEvent and persist audit record
        6. Enter or clear halts based on result
        """
        self._check_not_shutdown()

        import time

        t0 = time.monotonic()

        # -- 1. Gather local state --
        local_positions = await self._gather_local_positions()
        local_balances = await self._gather_local_balances()
        local_orders_list = await self._state_store.query_orders(limit=100_000)
        local_orders = {o.client_order_id: o for o in local_orders_list}
        local_fills_list = await self._state_store.query_fills(limit=100_000)
        local_fills: dict[str, list[FillRecord]] = {}
        for f in local_fills_list:
            local_fills.setdefault(f.client_order_id, []).append(f)

        # -- 2. Gather platform state --
        platform_positions = await self._fetch_platform_positions()
        platform_balances = await self._fetch_platform_balances()
        platform_orders = await self._fetch_platform_orders()
        platform_fills = await self._fetch_platform_fills()

        # -- 3. Detect mismatches --
        result = reconcile(
            local_positions=local_positions,
            platform_positions=platform_positions,
            local_balances=local_balances,
            platform_balances=platform_balances,
            local_orders=local_orders,
            platform_orders=platform_orders,
            local_fills=local_fills,
            platform_fills=platform_fills,
        )

        duration_ms = (time.monotonic() - t0) * 1000

        # -- 4. Apply resolution per case (Section 6.3) --
        await self._apply_reconciliation_result(result)

        # -- 5. Publish + audit --
        corr_id = _new_id()
        timestamp = _utcnow()

        self._event_bus.publish(
            ReconciliationCompleteEvent(
                event_id=_new_id(),
                timestamp=timestamp,
                adapter_name=self._adapter.platform_name,
                account_id=self._adapter.account_id,
                correlation_id=corr_id,
                mismatches=result.all_mismatches,
            )
        )

        await self._state_store.write_reconciliation_event(
            ReconciliationEvent(
                event_id=_new_id(),
                timestamp=timestamp,
                adapter_name=self._adapter.platform_name,
                account_id=self._adapter.account_id,
                correlation_id=corr_id,
                mismatches=result.all_mismatches,
                duration_ms=duration_ms,
            )
        )

        # -- 6. Halt management --
        await self._manage_halt_state(result, corr_id, timestamp)

        # -- 7. Adapter-owned user intent reconciliation --
        # Adapters that manage adapter-owned intent (e.g. Bybit leverage /
        # margin mode) detect and correct drift here.
        await self._adapter.reconcile_user_intent()

        return result

    async def _gather_local_positions(self) -> dict[Instrument, Position]:
        """Discover all positions by scanning position history."""
        history = await self._state_store.query_positions(limit=100_000)
        result: dict[Instrument, Position] = {}
        for pos in history:
            if pos.instrument not in result:
                result[pos.instrument] = pos
        return result

    async def _gather_local_balances(self) -> dict[str, Balance]:
        """Discover all balances by scanning balance history."""
        history = await self._state_store.query_balances(limit=100_000)
        result: dict[str, Balance] = {}
        for bal in history:
            if bal.currency not in result:
                result[bal.currency] = bal
        return result

    async def _fetch_platform_positions(self) -> dict[Instrument, Position]:
        try:
            return await self._adapter.fetch_positions()
        except NotImplementedError:
            return {}
        except Exception:
            logger.exception("Failed to fetch platform positions")
            return {}

    async def _fetch_platform_balances(self) -> dict[str, Balance]:
        try:
            return await self._adapter.fetch_balances()
        except NotImplementedError:
            return {}
        except Exception:
            logger.exception("Failed to fetch platform balances")
            return {}

    async def _fetch_platform_orders(self) -> dict[str, OrderRecord]:
        try:
            return await self._adapter.fetch_open_orders()
        except NotImplementedError:
            return {}
        except Exception:
            logger.exception("Failed to fetch platform orders")
            return {}

    async def _fetch_platform_fills(self) -> dict[str, list[FillRecord]]:
        try:
            return await self._adapter.fetch_fills()
        except NotImplementedError:
            return {}
        except Exception:
            logger.exception("Failed to fetch platform fills")
            return {}

    async def _apply_reconciliation_result(self, result: ReconciliationResult) -> None:
        """Apply resolution per mismatch case (Section 6.3).

        - Position mismatch  → overwrite local with platform truth
        - Balance mismatch   → overwrite local with platform truth
        - Orphan on platform → import into local state store
        - Orphan in local    → remove from local state store (not persisted
                               in v1 — we cannot delete rows from the audit
                               trail; the order is marked accordingly)
        - Partial fill diff  → overwrite local fills with platform fills
        """
        # Position mismatches: platform is authoritative
        for mm in result.position_mismatches:
            # Re-fetch platform position and overwrite local
            try:
                platform_positions = await self._adapter.fetch_positions()
                if mm.instrument and mm.instrument in platform_positions:
                    await self._state_store.upsert_position(platform_positions[mm.instrument])
            except Exception:
                logger.exception("Failed to overwrite position for %s", mm.instrument)

        # Balance mismatches: platform is authoritative
        for mm in result.balance_mismatches:
            try:
                platform_balances = await self._adapter.fetch_balances()
                for bal in platform_balances.values():
                    await self._state_store.upsert_balance(bal)
            except Exception:
                logger.exception("Failed to overwrite balances")

        # Orphan on platform: import into local
        for order in result.orphan_orders_on_platform:
            try:
                await self._state_store.upsert_order(order)
            except Exception:
                logger.exception("Failed to import orphan order %s", order.client_order_id)

        # Orphan in local: remove from local mirror (Section 6.3, case 4).
        # The audit trail is preserved — only current-state orders table is
        # cleaned up. The order record in audit_events remains immutable.
        for client_order_id in result.orphan_orders_in_local:
            try:
                await self._state_store.delete_orders_by_client_ids([client_order_id])
                logger.info(
                    "Removed orphan order %s from local mirror",
                    client_order_id,
                )
            except Exception:
                logger.exception(
                    "Failed to remove orphan order %s from local mirror",
                    client_order_id,
                )

        # Partial fill discrepancies: overwrite local fills with platform
        if result.partial_fill_discrepancies:
            try:
                platform_fills = await self._adapter.fetch_fills()
                await self._state_store.delete_fills_by_client_ids(list(platform_fills))
                all_fills = [fill for fills in platform_fills.values() for fill in fills]
                for fill in all_fills:
                    await self._state_store.upsert_fill(fill)
            except Exception:
                logger.exception("Failed to overwrite fills")

    async def _manage_halt_state(
        self,
        result: ReconciliationResult,
        corr_id: str,
        timestamp: datetime,
    ) -> None:
        """Enter halts on mismatches, clear halts on clean pass."""
        if result.is_clean:
            # Clear all active halts — iterate over them directly
            for entry in self._halt_machine.active_halts():
                cleared = self._halt_machine.try_clear_halt(
                    entry.scope,
                    instrument=entry.instrument,
                    reconciliation_is_clean=True,
                )
                if cleared:
                    self._event_bus.publish(
                        HaltClearedEvent(
                            event_id=_new_id(),
                            timestamp=timestamp,
                            adapter_name=self._adapter.platform_name,
                            account_id=self._adapter.account_id,
                            correlation_id=corr_id,
                            scope=entry.scope,
                            instrument=entry.instrument,
                            cleared_by="automatic",
                        )
                    )
                    await self._state_store.write_halt_event(
                        HaltEvent(
                            event_id=_new_id(),
                            timestamp=timestamp,
                            adapter_name=self._adapter.platform_name,
                            account_id=self._adapter.account_id,
                            correlation_id=corr_id,
                            action="cleared",
                            scope=entry.scope,
                            instrument=entry.instrument,
                            reason="reconciliation_clean",
                            detail="",
                            cleared_by="automatic",
                        )
                    )
            return

        if not self._halt_machine.config.auto_halt_enabled:
            return

        for mismatch in result.all_mismatches:
            scope: Literal["instrument", "account"] = (
                "instrument" if mismatch.instrument else "account"
            )
            inst = mismatch.instrument
            if self._halt_machine.enter_halt(
                scope=scope,
                instrument=inst,
                reason=mismatch.mismatch_type,
                detail=f"local={mismatch.local_value} platform={mismatch.platform_value}",
            ):
                self._event_bus.publish(
                    HaltEnteredEvent(
                        event_id=_new_id(),
                        timestamp=timestamp,
                        adapter_name=self._adapter.platform_name,
                        account_id=self._adapter.account_id,
                        correlation_id=corr_id,
                        scope=scope,
                        instrument=inst,
                        reason=mismatch.mismatch_type,
                        detail=f"local={mismatch.local_value} platform={mismatch.platform_value}",
                    )
                )
                await self._state_store.write_halt_event(
                    HaltEvent(
                        event_id=_new_id(),
                        timestamp=timestamp,
                        adapter_name=self._adapter.platform_name,
                        account_id=self._adapter.account_id,
                        correlation_id=corr_id,
                        action="entered",
                        scope=scope,
                        instrument=inst,
                        reason=mismatch.mismatch_type,
                        detail=f"local={mismatch.local_value} platform={mismatch.platform_value}",
                        cleared_by=None,
                    )
                )

    # ── State mirror access ────────────────────────────────────────

    async def get_position(self, instrument: Instrument) -> Position | None:
        return await self._state_store.get_position(instrument)

    async def get_balance(self, currency: str) -> Balance | None:
        return await self._state_store.get_balance(currency)

    # ── History accessors ──────────────────────────────────────────

    async def get_order_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[OrderRecord]:
        return await self._state_store.query_orders(
            instrument=instrument,
            start=start,
            end=end,
        )

    async def get_fill_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[FillRecord]:
        return await self._state_store.query_fills(
            instrument=instrument,
            start=start,
            end=end,
        )

    async def get_position_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Position]:
        return await self._state_store.query_positions(
            instrument=instrument,
            start=start,
            end=end,
        )

    async def get_balance_history(
        self,
        currency: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[Balance]:
        return await self._state_store.query_balances(
            currency=currency,
            start=start,
            end=end,
        )

    async def get_reconciliation_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[ReconciliationEvent]:
        return await self._state_store.query_reconciliation_events(
            start=start,
            end=end,
        )

    async def get_halt_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[HaltEvent]:
        return await self._state_store.query_halt_events(
            start=start,
            end=end,
        )

    async def get_audit_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[AuditEvent]:
        return await self._state_store.query_audit_events(
            start=start,
            end=end,
        )

    # ── Properties ─────────────────────────────────────────────────

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    @property
    def state_store(self) -> StateStore:
        return self._state_store

    @property
    def adapter(self) -> Adapter:
        return self._adapter

    @property
    def halt_machine(self) -> HaltStateMachine:
        return self._halt_machine

    @property
    def risk_config(self) -> RiskConfig:
        return self._risk_config

    # ── Internal: instrument spec caching ──────────────────────────

    async def _get_or_fetch_spec(self, instrument: Instrument) -> InstrumentSpec:
        if instrument not in self._instrument_specs:
            self._instrument_specs[instrument] = await self._adapter.fetch_instrument_spec(
                instrument
            )
        return self._instrument_specs[instrument]

    # ── Internal: reference price resolution ───────────────────────

    def _resolve_reference_price(self, instrument: Instrument) -> Decimal | None:
        if self._get_reference_price is None:
            return None
        return self._get_reference_price(instrument)

    # ── Internal: rate-limit tracking ──────────────────────────────

    def _effective_budget(self) -> int:
        if self._risk_config.rate_limit_budget_override is not None:
            return self._risk_config.rate_limit_budget_override
        return self._rate_limit_budget

    async def _refresh_rate_limits(self) -> None:
        try:
            rl = await self._adapter.get_rate_limits()
        except Exception:
            logger.warning("Failed to fetch rate limits from adapter")
            return
        self._rate_limit_budget = rl.remaining
        self._rate_limit_reset_at = rl.reset_at

    async def _refresh_rate_limits_if_stale(self) -> None:
        if self._rate_limit_reset_at is None:
            await self._refresh_rate_limits()
            return
        if self._rate_limit_budget > 0:
            return
        if _utcnow() >= self._rate_limit_reset_at:
            await self._refresh_rate_limits()

    # ── Internal: EventBus subscribers (state mirror) ──────────────

    def _on_fill(self, event: FillEvent) -> None:
        asyncio.ensure_future(self._persist_fill(event))

    def _on_position_update(self, event: PositionUpdateEvent) -> None:
        asyncio.ensure_future(self._persist_position(event))

    def _on_balance_update(self, event: BalanceUpdateEvent) -> None:
        asyncio.ensure_future(self._persist_balance(event))

    async def _persist_fill(self, event: FillEvent) -> None:
        try:
            await self._state_store.upsert_fill(event.fill)
        except Exception:
            logger.exception("Failed to persist fill %s", event.event_id)

    async def _persist_position(self, event: PositionUpdateEvent) -> None:
        try:
            await self._state_store.upsert_position(event.position)
        except Exception:
            logger.exception("Failed to persist position %s", event.event_id)

    async def _persist_balance(self, event: BalanceUpdateEvent) -> None:
        try:
            await self._state_store.upsert_balance(event.balance)
        except Exception:
            logger.exception("Failed to persist balance %s", event.event_id)

    # ── Internal: guards ───────────────────────────────────────────

    def _check_not_shutdown(self) -> None:
        if self._shutdown:
            raise EngineShutdownError("Engine has been shut down and is permanently unusable.")
