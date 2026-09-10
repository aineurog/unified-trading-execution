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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
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
    AccountChangedEvent,
    AuditEvent,
    BalanceUpdateEvent,
    ConnectionStateEvent,
    EventBus,
    FillEvent,
    HaltClearedEvent,
    HaltEnteredEvent,
    HaltEvent,
    OrderStatusEvent,
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

# Default fill settle lag: the leading edge of the reconciliation fill window
# (in seconds) that is left un-compared, because an execution-stream event can
# land a beat after the REST snapshot already sees the fill.  Comparing that
# still-in-flight tail would flag a phantom ``partial_fill`` for an
# immediately-filled order or its native TP/SL child.  Pass
# ``fill_settle_lag_seconds=0`` to compare fills up to "now" (no settle window).
DEFAULT_FILL_SETTLE_LAG_SECONDS: float = 5.0


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
    fill_compare_end: datetime
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
        fill_settle_lag_seconds: float = DEFAULT_FILL_SETTLE_LAG_SECONDS,
    ) -> None:
        self._adapter = adapter
        # Section 6.2: the state store is resolved at connect time.  When the
        # user supplies none, one is created at the auto-derived
        # ``./unified_trading_execution_data/<platform>_<account>.db`` location,
        # keyed by the *resolved* account identity (see connect()).  Until then
        # it stays None.  Never hidden, never hardcoded; readable via
        # ``engine.state_store.path`` after connect.
        self._state_store: StateStore | None = state_store
        self._get_reference_price = get_reference_price
        self._event_bus = event_bus or EventBus()
        self._risk_config = risk_config or RiskConfig()
        self._halt_machine = HaltStateMachine(halt_config)
        self._shutdown = False
        # Give the adapter access to core-managed resources it can use before
        # connect (halt machine, event bus).  The state store is attached in
        # connect() once it exists, before the adapter's streams start.
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
        if fill_settle_lag_seconds < 0:
            raise ValueError(
                f"fill_settle_lag_seconds must be >= 0, got {fill_settle_lag_seconds}"
            )
        self._fill_settle_lag_seconds = fill_settle_lag_seconds
        self._reconcile_loop_task: asyncio.Task[None] | None = None
        # Serialises manual / reconnect / periodic reconciles so they never
        # run concurrently and never mutate the mirror at the same time.
        self._reconcile_lock = asyncio.Lock()
        # Fire-and-forget persistence tasks scheduled from synchronous
        # EventBus handlers.  Hold a strong reference so the loop cannot
        # garbage-collect a task before it writes its DB record.
        self._background_tasks: set[asyncio.Task[None]] = set()

        # Wire up state-mirror subscriptions
        self._event_bus.subscribe(FillEvent, self._on_fill)
        self._event_bus.subscribe(OrderStatusEvent, self._on_order_status)
        self._event_bus.subscribe(PositionUpdateEvent, self._on_position_update)
        self._event_bus.subscribe(BalanceUpdateEvent, self._on_balance_update)
        self._event_bus.subscribe(ConnectionStateEvent, self._on_connection_state)
        self._event_bus.subscribe(AccountChangedEvent, self._on_account_changed)

    # ── Lifecycle ──────────────────────────────────────────────────

    async def connect(self) -> None:
        """Connect the adapter, initialise the state store, seed caches."""
        # Resolve the state store before the adapter's streams start.  If the
        # user supplied no store, one is derived from the *resolved* account
        # identity — so two accounts of the same platform never share a file.
        # The store is attached to the adapter first, so websocket handlers
        # (which may persist intent / halt state) see it from the first message.
        if self._state_store is None:
            account_id = await self._resolve_account_id()
            self._state_store = SQLiteStateStore(
                default_state_store_path(self._adapter.platform_name, account_id)
            )
        self._adapter.attach_state_store(self._state_store)

        await self._store.initialize()
        await self._adapter.connect()

        # Seed the position/balance mirror from platform truth so read-throughs
        # (get_positions / get_balance) reflect the platform before the first
        # reconciliation pass.  Best-effort: never raises.
        await self._seed_local_mirror()

        # Seed known order IDs from existing state
        try:
            existing = await self._store.query_orders(limit=100_000)
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

    async def _resolve_account_id(self) -> str:
        """Resolve the canonical platform account identity for store-path keying.

        Adapters whose configured label is not itself a unique platform identity
        (e.g. Bybit) override ``resolve_account_id`` to fetch the real id.  It
        must degrade gracefully rather than raise, but a defensive fallback to
        the configured ``account_id`` keeps connect() alive even on a misbehaving
        implementation.
        """
        try:
            return await self._adapter.resolve_account_id()
        except Exception:
            logger.warning("Could not resolve account id; falling back to configured account_id")
            return self._adapter.account_id

    async def _seed_local_mirror(self) -> None:
        """Import platform positions/balances into the mirror on connect.

        The mirror otherwise starts empty (or stale) until the first
        reconciliation pass, so a fresh connect would report no positions and
        zero balances to read-through queries.  This seeds both datasets
        immediately after the adapter is connected.  It is deliberately
        best-effort — an unsupported dataset (``NotImplementedError``) is
        skipped and any other failure is logged, never raised — so a flaky
        platform call cannot take :meth:`connect` down.
        """
        positions: list[Position] | None = None
        try:
            positions = await self._adapter.fetch_positions()
        except NotImplementedError:
            pass
        except Exception:
            logger.warning("Could not seed positions from platform on connect", exc_info=True)

        if positions:
            for pos in positions:
                try:
                    await self._store.upsert_position(pos)
                except Exception:
                    logger.exception("Failed to seed position %s", pos.position_id)

        balances: dict[str, Balance] | None = None
        try:
            balances = await self._adapter.fetch_balances()
        except NotImplementedError:
            pass
        except Exception:
            logger.warning("Could not seed balances from platform on connect", exc_info=True)

        if balances:
            for bal in balances.values():
                try:
                    await self._store.upsert_balance(bal)
                except Exception:
                    logger.exception("Failed to seed balance %s", bal.currency)

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
            await self._store.flush()
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
        if self._state_store is not None:
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
                state_store=self._store,
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

        # A synchronously-filled order's position leg can lag the fill on the
        # WS by a beat; seed it now so a reconcile running in that gap cannot
        # flag a phantom position mismatch (and enter a false instrument halt).
        if result.filled_quantity > 0:
            await self._seed_position_after_fill(order.instrument)

        return result

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Modify an existing order — risk-checked before dispatch."""
        self._check_not_shutdown()

        existing = await self._store.get_order(modification.client_order_id)
        if existing is None:
            from unified_trading_execution.errors import OrderNotFoundError

            raise OrderNotFoundError(modification.client_order_id)

        reference_price = self._resolve_reference_price(existing.instrument)
        await self._refresh_rate_limits_if_stale()

        # Exclude the order being modified from the duplicate check
        mod_known_ids = self._known_order_ids - {modification.client_order_id}

        result = await dispatch_modify_order(
            adapter=self._adapter,
            state_store=self._store,
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
            state_store=self._store,
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
        now = _utcnow()
        watermark = await self._store.get_reconcile_watermark()
        if watermark is None:
            watermark = now
        window_start = watermark

        # Fill settle lag: the execution stream can land a fill a beat after
        # the REST snapshot already sees it.  Compare only fills older than
        # ``fill_compare_end`` so a still-in-flight fill is never flagged as a
        # phantom ``partial_fill`` (and its WS event, when it lands, reconciles
        # cleanly next pass).  With a lag of 0 the boundary degenerates to
        # ``now`` — fills are compared up to the pass start.
        fill_compare_end = now - timedelta(seconds=self._fill_settle_lag_seconds)

        # -- 1. Gather local state --
        local_positions = await self._gather_local_positions()
        local_balances = await self._gather_local_balances()
        local_orders_list = await self._store.query_open_orders(limit=100_000)
        local_orders = {o.client_order_id: o for o in local_orders_list}
        local_fills_list = await self._store.query_fills(
            limit=100_000, start=window_start, end=fill_compare_end
        )
        local_fills: dict[str, list[FillRecord]] = {}
        for f in local_fills_list:
            local_fills.setdefault(f.client_order_id, []).append(f)

        # -- 2. Gather platform state (tri-state; may raise ReconciliationError) --
        platform_positions = await self._fetch_platform_positions()
        platform_balances = await self._fetch_platform_balances()
        platform_orders = await self._fetch_platform_orders()
        platform_fills = await self._fetch_platform_fills(since=window_start)
        if platform_fills is not None:
            # Bound the platform side to the same settle boundary as the local
            # side so both snapshots cover the identical window.
            settled_fills: dict[str, list[FillRecord]] = {}
            for cid, fills in platform_fills.items():
                kept = [f for f in fills if f.fill_timestamp <= fill_compare_end]
                if kept:
                    settled_fills[cid] = kept
            platform_fills = settled_fills

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
            fill_compare_end=fill_compare_end,
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
        # The watermark trails ``now`` by the settle lag, so the trailing
        # in-flight tail is re-checked next pass once its WS event has landed.
        if result.is_clean:
            await self._store.set_reconcile_watermark(fill_compare_end)

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

        await self._store.write_reconciliation_event(
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
        return await self._store.query_positions(limit=100_000)

    async def _gather_local_balances(self) -> dict[str, Balance]:
        """Discover all balances by scanning balance history."""
        history = await self._store.query_balances(limit=100_000)
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
                    await self._store.upsert_position(pos)
                for local in context.local_positions:
                    if (local.instrument, local.position_id) not in platform_keys:
                        if local.position_id is not None:
                            await self._store.delete_position(
                                local.instrument, local.position_id
                            )
            except Exception:
                logger.exception("Failed to sync positions to platform truth")

        # Balance mismatches: platform is authoritative.  Local-only currencies
        # are zeroed.
        if result.balance_mismatches and context.platform_balances is not None:
            try:
                for bal in context.platform_balances.values():
                    await self._store.upsert_balance(bal)
                for cur in context.local_balances:
                    if cur not in context.platform_balances:
                        await self._store.upsert_balance(
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
                await self._store.upsert_order(order)
            except Exception:
                logger.exception("Failed to import orphan order %s", order.client_order_id)

        # Orphan in local: remove from the open mirror.  The append-only
        # order_history snapshot preserves the lifecycle record.
        if result.orphan_orders_in_local:
            try:
                await self._store.delete_orders_by_client_ids(result.orphan_orders_in_local)
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
                    await self._store.delete_fills_by_client_ids(
                        [cid], since=context.window_start, end=context.fill_compare_end
                    )
                    for fill in context.platform_fills.get(cid, []):
                        fill = await self._stamp_fill_correlation(fill)
                        await self._store.upsert_fill(fill)
                except Exception:
                    logger.exception("Failed to correct fills for order %s", cid)

    async def _manage_halt_state(
        self,
        result: ReconciliationResult,
        corr_id: str,
        timestamp: datetime,
    ) -> None:
        """Enter halts on position mismatches, clear halts that reconciled.

        Instrument halts are cleared *targeted*: as soon as a halted
        instrument's position no longer mismatches in a pass, it is released —
        independent of the global ``is_clean`` flag.  Balance, orphan and
        partial-fill drift must never pin an instrument halt open: balance is
        settled-cash only (see ``reconcile``), and the others are corrected
        without halting.  The account-scoped halt (account change) is only
        released on a fully clean pass or manually.
        """
        mismatched_instruments = {
            m.instrument for m in result.position_mismatches if m.instrument is not None
        }

        # 1. Targeted instrument-halt clear — a halted instrument whose
        #    position reconciled this pass is released immediately.
        for entry in list(self._halt_machine.active_halts()):
            if entry.scope != "instrument" or entry.instrument is None:
                continue
            if entry.instrument in mismatched_instruments:
                continue  # still mismatched — keep halted
            await self._clear_halt(entry.scope, entry.instrument, corr_id, timestamp)

        # 2. Enter halts for any (new) position disagreement.  ``enter_halt``
        #    is idempotent and honours ``auto_halt_enabled``.
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

        # 3. On a fully clean pass, release anything still halted (the account
        #    halt and any instrument halt not already cleared above).
        if result.is_clean:
            for entry in list(self._halt_machine.active_halts()):
                await self._clear_halt(entry.scope, entry.instrument, corr_id, timestamp)

    async def _clear_halt(
        self,
        scope: Literal["instrument", "account"],
        instrument: Instrument | None,
        corr_id: str,
        timestamp: datetime,
    ) -> bool:
        """Attempt to clear one halt; publish + persist on success."""
        cleared = self._halt_machine.try_clear_halt(
            scope,
            instrument=instrument,
            reconciliation_is_clean=True,
        )
        if not cleared:
            return False
        self._event_bus.publish(
            HaltClearedEvent(
                event_id=_new_id(),
                timestamp=timestamp,
                adapter_name=self._adapter.platform_name,
                account_id=self._adapter.account_id,
                correlation_id=corr_id,
                scope=scope,
                instrument=instrument,
                cleared_by="automatic",
            )
        )
        await self._store.write_halt_event(
            HaltEvent(
                event_id=_new_id(),
                timestamp=timestamp,
                adapter_name=self._adapter.platform_name,
                account_id=self._adapter.account_id,
                correlation_id=corr_id,
                action="cleared",
                scope=scope,
                instrument=instrument,
                reason="reconciliation_clean",
                detail="",
                cleared_by="automatic",
            )
        )
        await self._persist_halt_clear(scope, instrument)
        return True

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
        await self._store.write_halt_event(
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
            active = await self._store.get_active_halts()
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
            await self._store.upsert_halt(scope, instrument, reason, detail)
        except Exception:
            logger.warning("Failed to persist halt (scope=%s)", scope)

    async def _persist_halt_clear(
        self, scope: Literal["instrument", "account"], instrument: Instrument | None
    ) -> None:
        """Persist a cleared halt; best-effort."""
        try:
            await self._store.delete_halt(scope, instrument)
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
            await self._store.write_halt_event(
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
        return await self._store.get_positions(instrument)

    async def get_net_position(self, instrument: Instrument) -> Position | None:
        return await self._store.get_net_position(instrument)

    async def get_balance(self, currency: str) -> Balance | None:
        return await self._store.get_balance(currency)

    # ── History accessors ──────────────────────────────────────────

    async def get_order_history(
        self,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[OrderRecord]:
        return await self._store.query_orders(
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
        return await self._store.query_fills(
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
        return await self._store.query_balances(
            currency=currency,
            start=start,
            end=end,
        )

    async def get_reconciliation_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[ReconciliationEvent]:
        return await self._store.query_reconciliation_events(
            start=start,
            end=end,
        )

    async def get_halt_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[HaltEvent]:
        return await self._store.query_halt_events(
            start=start,
            end=end,
        )

    async def get_audit_events(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[AuditEvent]:
        return await self._store.query_audit_events(
            start=start,
            end=end,
        )

    # ── Properties ─────────────────────────────────────────────────

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus

    @property
    def state_store(self) -> StateStore | None:
        """The state store, or ``None`` until :meth:`connect` resolves it.

        When no store is supplied at construction, the engine creates one on
        first connect keyed by the resolved account identity — so this is None
        before connect in that case.  A caller-supplied store is available
        immediately.
        """
        return self._state_store

    @property
    def _store(self) -> StateStore:
        """The state store, guaranteed non-None once :meth:`connect` has run.

        Internal narrowing helper: every store access outside ``connect`` /
        ``ashutdown`` goes through this so the store's deferral to connect is
        invisible to the rest of the engine.  Reaching it before connect (or
        after shutdown) is a programming error, not a recoverable condition.
        """
        store = self._state_store
        if store is None:
            raise RuntimeError(
                "State store is not initialized; call connect() before using the engine"
            )
        return store

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

    def _schedule(self, awaitable: Awaitable[None]) -> None:
        """Schedule a fire-and-forget coroutine, keeping a strong reference.

        Synchronous EventBus handlers cannot ``await``; they schedule async
        persistence on the running loop.  Without a retained reference the
        task may be garbage-collected before it runs, silently dropping the
        DB write — so we track it until it completes.
        """
        task = asyncio.ensure_future(awaitable)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _on_fill(self, event: FillEvent) -> None:
        self._schedule(self._persist_fill(event))

    def _on_order_status(self, event: OrderStatusEvent) -> None:
        self._schedule(self._persist_order_status(event))

    def _on_position_update(self, event: PositionUpdateEvent) -> None:
        self._schedule(self._persist_position(event))

    def _on_balance_update(self, event: BalanceUpdateEvent) -> None:
        self._schedule(self._persist_balance(event))

    def _on_account_changed(self, event: AccountChangedEvent) -> None:
        """Enter the account halt synchronously (immediate order block) and
        persist it via the event loop so a restart rehydrates it."""
        self._halt_machine.enter_halt("account", None, "account_changed", event.detail)
        self._schedule(self._persist_account_change(event))

    async def _persist_account_change(self, event: AccountChangedEvent) -> None:
        """Persist the account-change halt (event log + current-state table)."""
        try:
            self._event_bus.publish(
                HaltEnteredEvent(
                    event_id=_new_id(),
                    timestamp=event.timestamp,
                    adapter_name=self._adapter.platform_name,
                    account_id=self._adapter.account_id,
                    correlation_id=None,
                    scope="account",
                    instrument=None,
                    reason="account_changed",
                    detail=event.detail,
                )
            )
            await self._store.write_halt_event(
                HaltEvent(
                    event_id=_new_id(),
                    timestamp=event.timestamp,
                    adapter_name=self._adapter.platform_name,
                    account_id=self._adapter.account_id,
                    correlation_id=None,
                    action="entered",
                    scope="account",
                    instrument=None,
                    reason="account_changed",
                    detail=event.detail,
                    cleared_by=None,
                )
            )
            await self._store.upsert_halt("account", None, "account_changed", event.detail)
        except Exception:
            logger.exception("Failed to persist account-change halt")

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
            order = await self._store.get_order(fill.client_order_id)
        except Exception:
            logger.exception("Failed to resolve correlation_id for fill %s", fill.platform_fill_id)
            return fill
        if order is None:
            return fill
        return replace(fill, correlation_id=order.correlation_id)

    async def _persist_fill(self, event: FillEvent) -> None:
        try:
            fill = await self._stamp_fill_correlation(event.fill)
            await self._store.upsert_fill(fill)
        except Exception:
            logger.exception("Failed to persist fill %s", event.event_id)

    async def _persist_order_status(self, event: OrderStatusEvent) -> None:
        order = event.order
        try:
            # The WS order carries only ``client_order_id``; preserve the richer
            # dispatch-time ``correlation_id`` already stored for the order so a
            # status update never overwrites the placing action's trace id.
            existing = await self._store.get_order(order.client_order_id)
            if existing is not None and existing.correlation_id:
                order = replace(order, correlation_id=existing.correlation_id)
            await self._store.upsert_order(order)
        except Exception:
            logger.exception("Failed to persist order status for %s", order.client_order_id)

    async def _persist_position(self, event: PositionUpdateEvent) -> None:
        try:
            position = event.position
            # A zero-quantity update carrying a position_id is a close signal
            # for that leg — delete it rather than store a synthetic flat row.
            if position.quantity == 0 and position.position_id is not None:
                await self._store.delete_position(position.instrument, position.position_id)
            else:
                await self._store.upsert_position(position)
        except Exception:
            logger.exception("Failed to persist position %s", event.event_id)

    async def _persist_balance(self, event: BalanceUpdateEvent) -> None:
        try:
            await self._store.upsert_balance(event.balance)
        except Exception:
            logger.exception("Failed to persist balance %s", event.event_id)

    async def _seed_position_after_fill(self, instrument: Instrument) -> None:
        """Seed the position leg right after a synchronously-filled order.

        The WS ``position`` stream is the primary source of position truth, but
        it can arrive a beat after a synchronous fill.  Pulling the platform's
        own REST snapshot for the filled instrument closes that window for the
        immediately-filled case, so a reconciliation running in the gap cannot
        flag a phantom ``position_quantity`` mismatch (and enter a false
        instrument halt).  Best-effort by design: a fetch or persist failure
        must never fail the order placement that triggered it.
        """
        try:
            positions = await self._adapter.fetch_positions()
        except NotImplementedError:
            return
        except Exception:
            logger.exception(
                "Failed to fetch positions to seed after fill for %s", instrument.symbol
            )
            return
        for position in positions:
            if position.instrument != instrument:
                continue
            try:
                await self._store.upsert_position(position)
            except Exception:
                logger.exception("Failed to seed position after fill for %s", instrument.symbol)

    # ── Internal: guards ───────────────────────────────────────────

    def _check_not_shutdown(self) -> None:
        if self._shutdown:
            raise EngineShutdownError("Engine has been shut down and is permanently unusable.")
