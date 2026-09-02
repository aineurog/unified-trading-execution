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
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter
from unified_trading_execution.dispatch import (
    dispatch_cancel_order,
    dispatch_modify_order,
    dispatch_place_order,
)
from unified_trading_execution.errors import EngineShutdownError, ReconciliationError
from unified_trading_execution.events import (
    AuditEvent,
    BalanceUpdateEvent,
    ConnectionStateEvent,
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
from unified_trading_execution.state.store import SQLiteStateStore, default_state_store_path
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

# Default cadence for the automatic reconciliation loop.  Reconciliation runs
# by default so drift is caught without a manual step; pass
# ``reconcile_interval_seconds=None`` to disable it entirely.
DEFAULT_RECONCILE_INTERVAL_SECONDS: float = 30.0


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _fill_discrepant_order_ids(
    local_fills: dict[str, list[FillRecord]],
    platform_fills: dict[str, list[FillRecord]],
) -> list[str]:
    """Return the client_order_ids whose summed fill quantity differs between
    the local mirror and the platform (the partial-fill discrepancy set)."""
    ids: list[str] = []
    for cid in set(local_fills.keys()) | set(platform_fills.keys()):
        local_total = sum((f.fill_quantity for f in local_fills.get(cid, [])), start=Decimal("0"))
        platform_total = sum(
            (f.fill_quantity for f in platform_fills.get(cid, [])), start=Decimal("0")
        )
        if local_total != platform_total:
            ids.append(cid)
    return ids


@dataclass(frozen=True, slots=True)
class _ReconcileContext:
    """Platform and local snapshots carried from the gather phase into the
    apply phase so resolution never re-fetches per mismatch."""

    window_start: datetime
    local_positions: list[Position]
    local_balances: dict[str, Balance]
    local_fills: dict[str, list[FillRecord]]
    platform_positions: list[Position] | None
    platform_balances: dict[str, Balance] | None
    platform_orders: dict[str, OrderRecord] | None
    platform_fills: dict[str, list[FillRecord]] | None


class Engine:
    """Async-native trading engine — the main entry point.

    Construction::

        engine = Engine(
            adapter=BybitAdapter(...),
            state_store=SQLiteStateStore("path/to/db"),  # optional — see below
            get_reference_price=my_price_fn,  # optional
            event_bus=EventBus(),             # optional (auto-created)
            risk_config=RiskConfig(...),      # optional (sensible defaults)
            halt_config=HaltConfig(...),      # optional (auto-halt enabled)
        )
        await engine.connect()

    ``state_store`` is optional (Section 6.2): when omitted, the engine creates
    a ``SQLiteStateStore`` at the auto-derived, user-visible default location
    ``./<project>_data/<platform>_<account>.db`` (relative to the process
    working directory).  The resolved path is always readable at runtime via
    ``engine.state_store.path``.

    Usage::

        order = UnifiedOrder(...)
        result = await engine.place_order(order)
        await engine.disconnect()
    """

    def __init__(
        self,
        adapter: Adapter,
        state_store: StateStore | None = None,
        *,
        get_reference_price: Callable[[Instrument], Decimal | None] | None = None,
        event_bus: EventBus | None = None,
        risk_config: RiskConfig | None = None,
        halt_config: HaltConfig | None = None,
        reconcile_interval_seconds: float | None = DEFAULT_RECONCILE_INTERVAL_SECONDS,
    ) -> None:
        self._adapter = adapter
        # Section 6.2: storage location is optional with a sensible default —
        # when the user supplies no store, one is created at the auto-derived
        # ``./unified_trading_execution_data/<platform>_<account>.db`` location.
        # Never hidden, never hardcoded; readable via ``engine.state_store.path``.
        self._state_store = state_store or SQLiteStateStore(
            default_state_store_path(
                adapter.platform_name,
                adapter.account_id,
            )
        )
        self._get_reference_price = get_reference_price
        self._event_bus = event_bus or EventBus()
        self._risk_config = risk_config or RiskConfig()
        self._halt_machine = HaltStateMachine(halt_config)
        self._shutdown = False
        # Give the adapter access to core-managed resources (halt machine,
        # event bus, and the automatically-created state store) so adapters
        # that persist intent (leverage/margin-mode) can use the shared store.
        self._adapter.attach_state_store(self._state_store)
        self._adapter.attach_halt_machine(self._halt_machine)
        self._adapter.attach_event_bus(self._event_bus)

        # Mutable cached state
        self._known_order_ids: set[str] = set()
        self._rate_limit_budget: int = 0
        self._rate_limit_reset_at: datetime | None = None
        self._rate_limit_refresh_lock = asyncio.Lock()
        self._last_connected: bool | None = None
        self._reconcile_task: asyncio.Task[None] | None = None

        # Periodic reconciliation (on by default; None disables it).
        if reconcile_interval_seconds is not None and reconcile_interval_seconds <= 0:
            raise ValueError(
                f"reconcile_interval_seconds must be > 0 or None, got {reconcile_interval_seconds}"
            )
        self._reconcile_interval_seconds = reconcile_interval_seconds
        self._reconcile_loop_task: asyncio.Task[None] | None = None
        # Serialises manual / reconnect / periodic reconciles so they never
        # run concurrently and never mutate the mirror at the same time.
        self._reconcile_lock = asyncio.Lock()

        # Wire up state-mirror subscriptions
        self._event_bus.subscribe(FillEvent, self._on_fill)
        self._event_bus.subscribe(PositionUpdateEvent, self._on_position_update)
        self._event_bus.subscribe(BalanceUpdateEvent, self._on_balance_update)
        self._event_bus.subscribe(ConnectionStateEvent, self._on_connection_state)

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

        # Restore any halts that were active when the engine last shut down
        # (Section 6.4) so a restart never silently drops a protective halt.
        await self._restore_halts_from_store()

        # Fetch initial rate limits
        await self._refresh_rate_limits()

        # Start the optional periodic reconciliation loop, if enabled.
        if self._reconcile_interval_seconds is not None:
            self._reconcile_loop_task = asyncio.ensure_future(self._reconcile_loop())

    def _on_connection_state(self, event: ConnectionStateEvent) -> None:
        """Trigger an automatic reconcile when the connection re-establishes.

        Section 6.1: reconciliation is triggered immediately after any
        reconnect, since a dropped connection is the highest-risk window for
        drift.  This is the core-side half of that contract — adapters only
        publish ``ConnectionStateEvent``; they never call ``reconcile``.

        The first connect is not treated as a reconnect: ``_last_connected`` is
        None before the adapter's initial ``connected=True``, and we only react
        on a False -> True transition we have observed ourselves.
        """
        if self._shutdown:
            return
        if event.connected and self._last_connected is False:
            # Already have one reconciliation in flight — don't stack them.
            if self._reconcile_task is None or self._reconcile_task.done():
                self._reconcile_task = asyncio.ensure_future(self._reconcile_on_reconnect())
        self._last_connected = event.connected

    async def _reconcile_on_reconnect(self) -> None:
        """Best-effort reconcile after a reconnect; never fatal on failure."""
        try:
            await self.reconcile()
        except Exception:
            logger.exception("Automatic reconciliation after reconnect failed")

    async def _reconcile_loop(self) -> None:
        """Periodic reconciliation loop (only runs when the user opted in).

        Runs until shutdown.  Each pass is best-effort: a failed pass (e.g. a
        transient platform error) is logged and retried on the next tick.  The
        loop skips while the adapter is disconnected, since the reconnect path
        already triggers a reconcile on re-establishment.
        """
        interval = self._reconcile_interval_seconds
        if interval is None:
            return
        while True:
            if self._shutdown:
                break
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            if not self._adapter.is_connected:
                continue
            try:
                await self.reconcile()
            except Exception:
                logger.warning("Periodic reconciliation failed", exc_info=True)

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
        # Step 3 — cancel any pending reconnect reconciliation before closing
        if self._reconcile_task is not None and not self._reconcile_task.done():
            self._reconcile_task.cancel()
        if self._reconcile_loop_task is not None and not self._reconcile_loop_task.done():
            self._reconcile_loop_task.cancel()
        # Step 4 — close state store
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
        4. Apply resolution per case (Section 6.3) using carried snapshots
        5. Advance the "clean through" watermark only on a clean pass
        6. Publish ReconciliationCompleteEvent and persist audit record
        7. Enter or clear halts based on result

        A supported dataset that fails to fetch aborts the whole pass with
        ``ReconciliationError`` (fail loud) before any mutation, so a transient
        platform error is never mistaken for "no drift".  An unsupported dataset
        (``NotImplementedError``) is skipped entirely.
        """
        self._check_not_shutdown()
        async with self._reconcile_lock:
            return await self._reconcile_locked()

    async def _reconcile_locked(self) -> ReconciliationResult:
        import time

        t0 = time.monotonic()

        # Watermark ("clean through"): gates the fill window.  Forward-only
        # bootstrap — on the first pass there is no persisted watermark, so we
        # treat "now" as the clean point and compare only fills newer than it.
        # Positions/balances/open-orders are always full current snapshots.
        watermark = await self._state_store.get_reconcile_watermark()
        if watermark is None:
            watermark = _utcnow()
        window_start = watermark

        # -- 1. Gather local state --
        local_positions = await self._gather_local_positions()
        local_balances = await self._gather_local_balances()
        local_orders_list = await self._state_store.query_open_orders(limit=100_000)
        local_orders = {o.client_order_id: o for o in local_orders_list}
        local_fills_list = await self._state_store.query_fills(limit=100_000, start=window_start)
        local_fills: dict[str, list[FillRecord]] = {}
        for f in local_fills_list:
            local_fills.setdefault(f.client_order_id, []).append(f)

        # -- 2. Gather platform state (tri-state; may raise ReconciliationError) --
        platform_positions = await self._fetch_platform_positions()
        platform_balances = await self._fetch_platform_balances()
        platform_orders = await self._fetch_platform_orders()
        platform_fills = await self._fetch_platform_fills(since=window_start)

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

        # -- 4. Apply resolution using the already-fetched snapshots --
        context = _ReconcileContext(
            window_start=window_start,
            local_positions=local_positions,
            local_balances=local_balances,
            local_fills=local_fills,
            platform_positions=platform_positions,
            platform_balances=platform_balances,
            platform_orders=platform_orders,
            platform_fills=platform_fills,
        )
        await self._apply_reconciliation_result(result, context)

        # -- 5. Advance watermark only on a clean pass --
        if result.is_clean:
            await self._state_store.set_reconcile_watermark(_utcnow())

        # -- 6. Publish + audit --
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

        # -- 7. Halt management --
        await self._manage_halt_state(result, corr_id, timestamp)

        # -- 8. Adapter-owned user intent reconciliation --
        # Adapters that manage adapter-owned intent (e.g. Bybit leverage /
        # margin mode) detect and correct drift here.
        await self._adapter.reconcile_user_intent()

        return result

    async def _gather_local_positions(self) -> list[Position]:
        """Return all open position legs from the live state mirror."""
        return await self._state_store.query_positions(limit=100_000)

    async def _gather_local_balances(self) -> dict[str, Balance]:
        """Discover all balances by scanning balance history."""
        history = await self._state_store.query_balances(limit=100_000)
        result: dict[str, Balance] = {}
        for bal in history:
            if bal.currency not in result:
                result[bal.currency] = bal
        return result

    async def _fetch_platform_positions(self) -> list[Position] | None:
        """Fetch platform positions (tri-state).

        ``NotImplementedError`` → unsupported (None, skip comparison).
        Any other error → fail loud (abort the whole pass, no mutation).
        """
        try:
            return await self._adapter.fetch_positions()
        except NotImplementedError:
            return None
        except Exception as exc:
            raise ReconciliationError(f"Failed to fetch platform positions: {exc}") from exc

    async def _fetch_platform_balances(self) -> dict[str, Balance] | None:
        """Fetch platform balances (tri-state)."""
        try:
            return await self._adapter.fetch_balances()
        except NotImplementedError:
            return None
        except Exception as exc:
            raise ReconciliationError(f"Failed to fetch platform balances: {exc}") from exc

    async def _fetch_platform_orders(self) -> dict[str, OrderRecord] | None:
        """Fetch platform open orders (tri-state)."""
        try:
            return await self._adapter.fetch_open_orders()
        except NotImplementedError:
            return None
        except Exception as exc:
            raise ReconciliationError(f"Failed to fetch platform open orders: {exc}") from exc

    async def _fetch_platform_fills(
        self, *, since: datetime | None
    ) -> dict[str, list[FillRecord]] | None:
        """Fetch platform fills since *since* (tri-state)."""
        try:
            return await self._adapter.fetch_fills(since=since)
        except NotImplementedError:
            return None
        except Exception as exc:
            raise ReconciliationError(f"Failed to fetch platform fills: {exc}") from exc

    async def _apply_reconciliation_result(
        self, result: ReconciliationResult, context: _ReconcileContext
    ) -> None:
        """Apply resolution per mismatch case (Section 6.3) using carried snapshots.

        Resolution never re-fetches platform state — it uses the snapshots
        gathered at the start of the pass.  Position/balance drift triggers a
        full sync of that dataset (platform truth imported, local-only entries
        zeroed).  Orphan and partial-fill corrections are surgical.
        """
        # Position mismatches: platform is authoritative.  Platform legs are
        # upserted and local-only legs are deleted (closed, not zeroed).
        if result.position_mismatches and context.platform_positions is not None:
            try:
                platform_keys = {(p.instrument, p.position_id) for p in context.platform_positions}
                for pos in context.platform_positions:
                    await self._state_store.upsert_position(pos)
                for local in context.local_positions:
                    if (local.instrument, local.position_id) not in platform_keys:
                        if local.position_id is not None:
                            await self._state_store.delete_position(
                                local.instrument, local.position_id
                            )
            except Exception:
                logger.exception("Failed to sync positions to platform truth")

        # Balance mismatches: platform is authoritative.  Local-only currencies
        # are zeroed.
        if result.balance_mismatches and context.platform_balances is not None:
            try:
                for bal in context.platform_balances.values():
                    await self._state_store.upsert_balance(bal)
                for cur in context.local_balances:
                    if cur not in context.platform_balances:
                        await self._state_store.upsert_balance(
                            Balance(
                                currency=cur,
                                free=Decimal("0"),
                                used=Decimal("0"),
                                total=Decimal("0"),
                                updated_at=_utcnow(),
                            )
                        )
            except Exception:
                logger.exception("Failed to sync balances to platform truth")

        # Orphan on platform: import into local.
        for order in result.orphan_orders_on_platform:
            try:
                await self._state_store.upsert_order(order)
            except Exception:
                logger.exception("Failed to import orphan order %s", order.client_order_id)

        # Orphan in local: remove from the open mirror.  The append-only
        # order_history snapshot preserves the lifecycle record.
        if result.orphan_orders_in_local:
            try:
                await self._state_store.delete_orders_by_client_ids(result.orphan_orders_in_local)
            except Exception:
                logger.exception("Failed to remove orphan orders from local mirror")
            else:
                for client_order_id in result.orphan_orders_in_local:
                    logger.info("Removed orphan order %s from local mirror", client_order_id)

        # Partial fill: surgical correction per discrepant order, bounded to the
        # watermark window so pre-watermark fills are never disturbed.
        if result.partial_fill_discrepancies and context.platform_fills is not None:
            for cid in _fill_discrepant_order_ids(context.local_fills, context.platform_fills):
                try:
                    await self._state_store.delete_fills_by_client_ids(
                        [cid], since=context.window_start
                    )
                    for fill in context.platform_fills.get(cid, []):
                        fill = await self._stamp_fill_correlation(fill)
                        await self._state_store.upsert_fill(fill)
                except Exception:
                    logger.exception("Failed to correct fills for order %s", cid)

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
                    await self._persist_halt_clear(entry.scope, entry.instrument)
            return

        if not self._halt_machine.config.auto_halt_enabled:
            return

        # Only position disagreements halt.  Balance drift is corrected
        # silently (platform truth is imported in _apply_reconciliation_result)
        # but never halts: account equity/margin float with live P&L, so a
        # balance delta is normal intraday movement, not evidence of
        # account-state corruption.  Orphan orders and partial-fill
        # discrepancies are likewise corrected without halting.
        for mismatch in result.position_mismatches:
            if mismatch.instrument is None:
                continue  # defensive: position mismatches always carry an instrument
            await self._enter_halt(
                scope="instrument",
                instrument=mismatch.instrument,
                reason=mismatch.mismatch_type,
                detail=f"local={mismatch.local_value} platform={mismatch.platform_value}",
                corr_id=corr_id,
                timestamp=timestamp,
            )

    async def _enter_halt(
        self,
        *,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None,
        reason: str,
        detail: str,
        corr_id: str,
        timestamp: datetime,
    ) -> None:
        """Enter a halt and publish/persist the corresponding events."""
        if not self._halt_machine.enter_halt(
            scope=scope, instrument=instrument, reason=reason, detail=detail
        ):
            return
        self._event_bus.publish(
            HaltEnteredEvent(
                event_id=_new_id(),
                timestamp=timestamp,
                adapter_name=self._adapter.platform_name,
                account_id=self._adapter.account_id,
                correlation_id=corr_id,
                scope=scope,
                instrument=instrument,
                reason=reason,
                detail=detail,
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
                instrument=instrument,
                reason=reason,
                detail=detail,
                cleared_by=None,
            )
        )
        await self._persist_halt(scope, instrument, reason, detail)

    async def _restore_halts_from_store(self) -> None:
        """Rehydrate persisted halts into the halt machine (Section 6.4)."""
        try:
            active = await self._state_store.get_active_halts()
        except Exception:
            logger.warning("Could not restore persisted halts from state store")
            return
        for scope, instrument, reason, detail in active:
            try:
                self._halt_machine.restore_halt(scope, instrument, reason, detail)
            except Exception:
                logger.warning("Could not restore halt (scope=%s) from state store", scope)

    async def _persist_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None,
        reason: str,
        detail: str,
    ) -> None:
        """Persist an entered halt; best-effort — never breaks the halt itself."""
        try:
            await self._state_store.upsert_halt(scope, instrument, reason, detail)
        except Exception:
            logger.warning("Failed to persist halt (scope=%s)", scope)

    async def _persist_halt_clear(
        self, scope: Literal["instrument", "account"], instrument: Instrument | None
    ) -> None:
        """Persist a cleared halt; best-effort."""
        try:
            await self._state_store.delete_halt(scope, instrument)
        except Exception:
            logger.warning("Failed to persist halt clear (scope=%s)", scope)

    # ── Manual halt clearing ───────────────────────────────────────

    async def clear_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None = None,
    ) -> bool:
        """Manually clear a halt for the given scope (Section 6.4).

        Works regardless of ``HaltClearMode``: it authorises the clear through
        both state-machine gates (``manual_clear`` for MANUAL mode and
        ``reconciliation_is_clean`` for AUTOMATIC mode) so an explicit user
        request always clears.  Returns True if a halt was actually cleared.
        """
        self._check_not_shutdown()
        cleared = self._halt_machine.try_clear_halt(
            scope,
            instrument=instrument,
            reconciliation_is_clean=True,
            manual_clear=True,
        )
        if cleared:
            corr_id = _new_id()
            timestamp = _utcnow()
            self._event_bus.publish(
                HaltClearedEvent(
                    event_id=_new_id(),
                    timestamp=timestamp,
                    adapter_name=self._adapter.platform_name,
                    account_id=self._adapter.account_id,
                    correlation_id=corr_id,
                    scope=scope,
                    instrument=instrument,
                    cleared_by="manual",
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
                    scope=scope,
                    instrument=instrument,
                    reason="manual_clear",
                    detail="",
                    cleared_by="manual",
                )
            )
            await self._persist_halt_clear(scope, instrument)
        return cleared

    # ── State mirror access ────────────────────────────────────────

    async def get_positions(self, instrument: Instrument) -> list[Position]:
        return await self._state_store.get_positions(instrument)

    async def get_net_position(self, instrument: Instrument) -> Position | None:
        return await self._state_store.get_net_position(instrument)

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
        position_id: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[FillRecord]:
        return await self._state_store.query_fills(
            instrument=instrument,
            position_id=position_id,
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

    # ── Adapter method auto-proxy ─────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        """Proxy unknown attribute lookups to the underlying adapter.

        Adapter-specific methods (``fetch_account_leverage``, ``set_leverage``,
        ``fetch_positions``, ...) are not on the ``Adapter`` ABC — they vary by
        platform.  This returns the adapter's coroutine directly (the caller
        awaits it), mirroring how ``SyncEngine`` proxies through its background
        loop.  Core never imports adapter code — resolution is dynamic.
        """
        adapter = self.__dict__.get("_adapter")
        if adapter is None:
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
        method = getattr(adapter, name, None)
        if callable(method):
            return method
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    # ── Internal: instrument spec caching ──────────────────────────

    async def _get_or_fetch_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Return the adapter's cached spec — the adapter manages TTL and invalidation."""
        return await self._adapter.fetch_instrument_spec(instrument)

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

    async def _stamp_fill_correlation(self, fill: FillRecord) -> FillRecord:
        """Stamp a fill with the placing action's correlation_id (Section 17.14).

        The adapter can recover only ``client_order_id`` from the deal comment,
        so it cannot know the dispatch-time ``correlation_id``.  The engine
        resolves it here from the persisted order snapshot.  Unknown tickets
        (empty ``client_order_id``, or no local order) keep the adapter's
        ``client_order_id`` fallback.
        """
        if not fill.client_order_id:
            return fill
        try:
            order = await self._state_store.get_order(fill.client_order_id)
        except Exception:
            logger.exception("Failed to resolve correlation_id for fill %s", fill.platform_fill_id)
            return fill
        if order is None:
            return fill
        return replace(fill, correlation_id=order.correlation_id)

    async def _persist_fill(self, event: FillEvent) -> None:
        try:
            fill = await self._stamp_fill_correlation(event.fill)
            await self._state_store.upsert_fill(fill)
        except Exception:
            logger.exception("Failed to persist fill %s", event.event_id)

    async def _persist_position(self, event: PositionUpdateEvent) -> None:
        try:
            position = event.position
            # A zero-quantity update carrying a position_id is a close signal
            # for that leg — delete it rather than store a synthetic flat row.
            if position.quantity == 0 and position.position_id is not None:
                await self._state_store.delete_position(position.instrument, position.position_id)
            else:
                await self._state_store.upsert_position(position)
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
