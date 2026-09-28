"""HyperliquidAdapter — concrete Adapter ABC implementation for Hyperliquid.

Covers Hyperliquid spot and perpetual markets.  One ``Exchange`` instance
per adapter (one account per adapter; the SDK call path is blocking, so
every call goes through ``asyncio.to_thread`` with the configured request
timeout and is never awaited on the loop thread).  Identity is the wallet
address itself — no separate uid-resolve step.  One-way positions only —
no hedge routing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import requests
from hyperliquid.exchange import Exchange
from hyperliquid.utils.constants import MAINNET_API_URL, TESTNET_API_URL
from hyperliquid.utils.error import ClientError, ServerError
from hyperliquid.utils.types import Cloid
from uuid_extensions import uuid7

from unified_trading_execution.adapter import Adapter, RateLimits
from unified_trading_execution.errors import (
    InvalidOrderError,
    InvalidSymbolError,
    OrderNotFoundError,
    PlatformConnectionError,
    PlatformError,
)
from unified_trading_execution.events import ConnectionStateEvent, Event, EventBus
from unified_trading_execution.hyperliquid.config import HyperliquidConfig
from unified_trading_execution.hyperliquid.enums import MarginMode
from unified_trading_execution.hyperliquid.errors import map_hyperliquid_error
from unified_trading_execution.hyperliquid.orders import (
    MAX_DECIMALS_PERPS,
    MAX_DECIMALS_SPOT,
    SL_CLOID_SUFFIX,
    TP_CLOID_SUFFIX,
    build_cancel_action,
    build_modify_action,
    build_place_order_action,
    client_order_id_to_cloid,
    max_limit_notional,
    max_market_notional,
    parse_order_result,
    quantize_price,
    raise_on_status_errors,
    round_price_to_tick,
    validate_size,
)
from unified_trading_execution.hyperliquid.signing import (
    assert_user_role_for_signing,
    build_wallet,
)
from unified_trading_execution.hyperliquid.streams import (
    translate_balance,
    translate_fill,
    translate_order_entry,
    translate_position,
    translate_ticker,
)
from unified_trading_execution.hyperliquid.symbols import (
    from_hyperliquid_coin,
    to_hyperliquid_coin,
)
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.types.enums import (
    AssetClass,
    FillEntry,
    FillReason,
    OrderSide,
    OrderStatus,
    OrderType,
)
from unified_trading_execution.types.instrument import Instrument, InstrumentSpec
from unified_trading_execution.types.market_data import Ticker
from unified_trading_execution.types.order import (
    FillRecord,
    OrderModification,
    OrderRecord,
    OrderResult,
    TpSlAttachment,
    UnifiedOrder,
)
from unified_trading_execution.types.position import Balance, Position

logger = logging.getLogger(__name__)


def _new_id() -> str:
    return str(uuid7())


def _utcnow() -> datetime:
    return datetime.now(UTC)


class HyperliquidAdapter(Adapter):
    """Concrete Adapter ABC implementation for Hyperliquid."""

    def __init__(
        self,
        config: HyperliquidConfig,
        *,
        event_bus: EventBus | None = None,
        state_store: StateStore | None = None,
    ) -> None:
        self._config = config
        self._event_bus = event_bus
        self._state_store = state_store
        self._halt_machine: HaltStateMachine | None = None
        self._connected = False
        self._exchange: Exchange | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        # client_order_id -> (coin, is_spot), populated at place time so
        # modify/cancel can address orders without a venue scan.  Unknown
        # ids fall back to scanning open orders for the cloid.
        self._client_coins: dict[str, tuple[str, bool]] = {}
        # platform oid -> (client_order_id, reason, entry) for fill
        # attribution; child-cloid raw -> (parent id, reason, entry).
        self._oid_clients: dict[str, tuple[str, FillReason | None, FillEntry | None]] = {}
        self._child_parents: dict[str, tuple[str, FillReason, FillEntry]] = {}
        # InstrumentSpec cache with monotonic fetch times (TTL-governed).
        self._instrument_specs: dict[Instrument, tuple[InstrumentSpec, float]] = {}
        self._spec_ttl: float | None = config.instrument_spec_cache_ttl

    # ---- Identification ----

    @property
    def platform_name(self) -> str:
        return self._config.platform_name

    @property
    def account_id(self) -> str:
        """The wallet address is the canonical account identity."""
        return self._config.wallet_address

    async def resolve_account_id(self) -> str:
        """Return the canonical identity — the wallet address itself."""
        return self._config.wallet_address

    # ---- Adapter-owned wiring ----

    def attach_halt_machine(self, halt_machine: HaltStateMachine | None) -> None:
        """Store core's shared halt state machine so drift can enter halts."""
        self._halt_machine = halt_machine

    def attach_event_bus(self, event_bus: EventBus) -> None:
        """Store the engine's shared event bus so WS handlers can publish."""
        self._event_bus = event_bus

    def attach_state_store(self, state_store: StateStore) -> None:
        """Receive the engine-managed StateStore for per-asset intent."""
        self._state_store = state_store

    # ---- Connection lifecycle ----

    def _publish(self, event: Event) -> None:
        """Publish onto the engine's bus, requiring it was wired first."""
        if self._event_bus is None:
            raise RuntimeError(
                "event_bus not wired — construct via Engine or call attach_event_bus() first"
            )
        self._event_bus.publish(event)

    def _publish_connection_state(self, connected: bool) -> None:
        self._publish(
            ConnectionStateEvent(
                event_id=_new_id(),
                timestamp=_utcnow(),
                adapter_name=self.platform_name,
                account_id=self.account_id,
                correlation_id=None,
                connected=connected,
            )
        )

    def _require_exchange(self) -> Exchange:
        if self._exchange is None:
            raise PlatformConnectionError("Hyperliquid adapter is not connected")
        return self._exchange

    async def connect(self) -> None:
        """Open the transport and verify signing identity.

        Builds the SDK ``Exchange`` (which fetches ``meta``/``spotMeta``
        over the network at construction, hence off-loop) with the
        configured timeout, then asserts the signing key's ``userRole``
        (``approveAgent`` one-time setup) and the account's
        ``userAbstraction`` (unified only).  Publishes
        ``ConnectionStateEvent(connected=True)`` on success.  WebSocket
        subscriptions attach in a later step; this only opens REST.
        """
        if self._connected:
            return
        self._loop = asyncio.get_running_loop()
        wallet = build_wallet(self._config.private_key)
        base_url = TESTNET_API_URL if self._config.testnet else MAINNET_API_URL
        try:
            exchange = await asyncio.to_thread(
                Exchange,
                wallet,
                base_url,
                None,
                None,
                self._config.wallet_address,
                None,
                None,
                self._config.request_timeout_seconds,
            )
        except ClientError as exc:
            raise PlatformError(f"Hyperliquid transport rejected the connection: {exc}") from exc
        except (ServerError, requests.exceptions.RequestException) as exc:
            raise PlatformConnectionError(f"Hyperliquid connection failed: {exc}") from exc

        try:
            role = await asyncio.to_thread(exchange.info.user_role, wallet.address)
            assert_user_role_for_signing((role or {}).get("role"))
            abstraction = await asyncio.to_thread(
                exchange.info.query_user_abstraction_state, self._config.wallet_address
            )
            if abstraction != "unifiedAccount":
                raise PlatformError(
                    f"Hyperliquid account abstraction {abstraction!r} is not supported — "
                    "unified account only"
                )
        except (PlatformError, PlatformConnectionError):
            raise
        except ClientError as exc:
            raise PlatformError(f"Hyperliquid identity check rejected: {exc}") from exc
        except (ServerError, requests.exceptions.RequestException) as exc:
            raise PlatformConnectionError(f"Hyperliquid identity check failed: {exc}") from exc

        self._exchange = exchange
        self._connected = True
        await self._reapply_stored_intent()
        self._publish_connection_state(True)

    async def disconnect(self) -> None:
        """Close the transport gracefully.

        Publishes ``ConnectionStateEvent(connected=False)``.  WebSocket
        teardown joins here when subscriptions land.
        """
        if self._exchange is None and not self._connected:
            return
        self._exchange = None
        self._connected = False
        self._client_coins.clear()
        self._oid_clients.clear()
        self._child_parents.clear()
        self._publish_connection_state(False)

    @property
    def is_connected(self) -> bool:
        """Return True if the transport is currently established."""
        return self._connected

    # ---- Transport helper ----

    async def _run_exchange(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking SDK call off-loop with unified error mapping.

        Transport failures (HTTP 5xx, timeouts, drops) become
        ``PlatformConnectionError``; client errors (4xx) become
        ``PlatformError``.  Venue ``{"status": "err"}`` envelopes do NOT
        raise here — the caller extracts and maps them via
        :meth:`_check_action_ok`, since only the caller knows the context.
        """
        self._require_exchange()
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        except ClientError as exc:
            raise PlatformError(f"Hyperliquid request rejected: {exc}") from exc
        except (ServerError, requests.exceptions.RequestException) as exc:
            raise PlatformConnectionError(f"Hyperliquid request failed: {exc}") from exc

    @staticmethod
    def _check_action_ok(response: Any, *, context: str) -> Any:
        """Unwrap an action response or raise its mapped error.

        A top-level ``{"status": "err", "response": "<string>"}`` envelope
        (observed live on cancels and auth failures) maps through the error
        table; anything not shaped like a response raises generic
        ``PlatformError`` with context.
        """
        if not isinstance(response, dict):
            raise PlatformError(f"Unexpected Hyperliquid response {response!r} ({context})")
        if response.get("status") == "err":
            data = response.get("response")
            message = data if isinstance(data, str) else context
            raise map_hyperliquid_error(message=message)
        return response

    @staticmethod
    def _is_spot_coin(coin: str) -> bool:
        """Classify a coin by name encoding (aliases/pairs are spot, bare names perps).

        Biconditional with the SDK's own ``asset >= 10_000`` rule for every
        in-scope coin (verified across listings); used where the coin is
        known but the id is not yet resolved.
        """
        return coin.startswith("@") or "/" in coin

    def _reverse_pair_coins(self) -> dict[str, str]:
        """Pair table for alias decoding, viewed off the SDK index.

        A per-call copy rather than a stored second registry — nothing to
        sync, test, or diverge, since the SDK builds its index once.
        """
        exchange = self._require_exchange()
        return dict(exchange.info.name_to_coin)

    # ---- Order operations ----

    async def _find_open_entry(
        self, client_order_id: str
    ) -> tuple[dict[str, Any], str, bool] | None:
        """Find the newest open entry carrying the cloid, with its coin.

        Timestamp-max over matches so echo duplicates resolve to the live
        leg, never first-match.  Returns ``(entry, coin, is_spot)`` or None.
        """
        want = client_order_id_to_cloid(client_order_id)
        best: dict[str, Any] | None = None
        best_timestamp = -1
        for entry in await self._open_order_entries():
            if not isinstance(entry, dict):
                continue
            if str(entry.get("cloid") or "") != want:
                continue
            coin = str(entry.get("coin") or "")
            if not coin:
                continue
            try:
                timestamp = int(str(entry.get("timestamp") or "0"))
            except ValueError:
                timestamp = 0
            if timestamp >= best_timestamp:
                best, best_timestamp = entry, timestamp
        if best is None:
            return None
        coin = str(best.get("coin") or "")
        return best, coin, self._is_spot_coin(coin)

    async def _resolve_coin(self, client_order_id: str) -> tuple[str, bool]:
        """Resolve the ``(coin, is_spot)`` for a client order id.

        Placement cache first; otherwise scan open orders for the cloid
        (covers restarts and orders placed outside this session).
        """
        cached = self._client_coins.get(client_order_id)
        if cached is not None:
            return cached
        found = await self._find_open_entry(client_order_id)
        if found is None:
            raise OrderNotFoundError(f"Order {client_order_id} not found")
        _, coin, is_spot = found
        resolved = (coin, is_spot)
        self._client_coins[client_order_id] = resolved
        return resolved

    async def _open_order_entries(self) -> list[Any]:
        """Fetch raw open-order entries across both order shapes."""
        exchange = self._require_exchange()
        address = self._config.wallet_address
        basic = await self._run_exchange(exchange.info.open_orders, address)
        frontend = await self._run_exchange(exchange.info.frontend_open_orders, address)
        entries: list[Any] = []
        if isinstance(basic, list):
            entries.extend(basic)
        if isinstance(frontend, list):
            entries.extend(frontend)
        return entries

    async def _current_order_record(self, client_order_id: str) -> OrderRecord:
        """Fetch the live ``OrderRecord`` for modify-merge; missing becomes not-found."""
        found = await self._find_open_entry(client_order_id)
        if found is None:
            raise OrderNotFoundError(f"Order {client_order_id} is not open")
        entry, coin, is_spot = found
        instrument = from_hyperliquid_coin(
            coin, is_spot=is_spot, spot_pair_coins=self._reverse_pair_coins()
        )
        return translate_order_entry(entry, instrument=instrument)

    async def _market_band_price(
        self, coin: str, side: OrderSide, *, is_spot: bool, sz_decimals: int
    ) -> Decimal:
        """Touch-derived aggressive price for MARKET orders (up buys, down sells)."""
        exchange = self._require_exchange()
        book = await self._run_exchange(exchange.info.l2_snapshot, coin)
        try:
            levels = book["levels"]
            touch = levels[1][0] if side == OrderSide.BUY else levels[0][0]
            touch_px = Decimal(str(touch["px"]))
        except (KeyError, IndexError, TypeError) as exc:
            raise map_hyperliquid_error(message="No liquidity available for market order.") from exc
        band = touch_px * (Decimal("1.01") if side == OrderSide.BUY else Decimal("0.99"))
        return round_price_to_tick(
            band,
            sz_decimals,
            is_spot=is_spot,
            direction="up" if side == OrderSide.BUY else "down",
        )

    async def _refresh_oid_index(self) -> None:
        """Rebuild oid→client attribution from open orders carrying our cloids.

        Covers legs whose oids were never acked (``waitingForTrigger``
        children) and sessions restarted after placement.  Unknown oids
        stay unattributed — fetch_fills keys those by raw oid.
        """
        for entry in await self._open_order_entries():
            if not isinstance(entry, dict):
                continue
            raw = str(entry.get("cloid") or "")
            oid = entry.get("oid")
            if not raw or oid is None or oid == "":
                continue
            if raw in self._child_parents:
                parent, reason, entry_side = self._child_parents[raw]
                self._oid_clients[str(oid)] = (parent, reason, entry_side)
                continue
            for client_order_id in list(self._client_coins):
                if raw == client_order_id_to_cloid(client_order_id):
                    self._oid_clients[str(oid)] = (client_order_id, None, None)
                    break

    async def place_order(self, order: UnifiedOrder) -> OrderResult:
        """Translate and submit a fully-validated order.

        Receives a ``UnifiedOrder`` that has already passed all risk checks.
        Sizes/prices validate against live ``szDecimals`` (strict — never
        reshaped); MARKET pricing is touch-derived automatically; notionals
        check against the live max-leverage tier.  Returns the parent
        result; TP/SL legs ride the same action.  ``cloid``-addressed
        end-to-end for idempotent retry.
        """
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(order.instrument)
        is_spot = order.instrument.asset_class == AssetClass.SPOT
        spec = await self.fetch_instrument_spec(order.instrument)
        lot_exponent = spec.lot_size.as_tuple().exponent
        if not isinstance(lot_exponent, int):
            raise PlatformError(f"InstrumentSpec has no usable lot_size for {coin}")
        sz_decimals = -lot_exponent

        client_order_id = order.client_order_id or _new_id()
        validate_size(order.quantity, sz_decimals)
        if order.price is not None:
            quantize_price(order.price, sz_decimals, is_spot=is_spot)
        if order.stop_price is not None:
            quantize_price(order.stop_price, sz_decimals, is_spot=is_spot)
        for attachment in (order.take_profit, order.stop_loss):
            if attachment is None:
                continue
            quantize_price(attachment.trigger_price, sz_decimals, is_spot=is_spot)
            if attachment.limit_price is not None:
                quantize_price(attachment.limit_price, sz_decimals, is_spot=is_spot)

        market_limit_price: Decimal | None = None
        if order.order_type == OrderType.MARKET:
            market_limit_price = await self._market_band_price(
                coin, order.side, is_spot=is_spot, sz_decimals=sz_decimals
            )
            if not is_spot and spec.max_leverage is not None:
                cap = max_market_notional(int(spec.max_leverage))
                if market_limit_price * order.quantity > cap:
                    raise InvalidOrderError(f"Market notional exceeds tier cap {cap} for {coin}")
        elif (
            order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT)
            and not is_spot
            and spec.max_leverage is not None
        ):
            assert order.price is not None
            cap = max_limit_notional(int(spec.max_leverage))
            if order.price * order.quantity > cap:
                raise InvalidOrderError(f"Limit notional exceeds tier cap {cap} for {coin}")

        requests, grouping = build_place_order_action(
            order,
            coin=coin,
            client_order_id=client_order_id,
            market_limit_price=market_limit_price,
        )
        for request in requests:
            request["cloid"] = Cloid(request["cloid"])
        response = await self._run_exchange(exchange.bulk_orders, requests, grouping=grouping)
        data = self._check_action_ok(response, context=f"placing order {client_order_id}")
        try:
            statuses = data["response"]["data"]["statuses"]
        except (KeyError, TypeError) as exc:
            raise PlatformError(
                f"Unexpected order response shape {data!r} for {client_order_id}"
            ) from exc
        if not isinstance(statuses, list) or not statuses:
            raise PlatformError(f"Empty order statuses for {client_order_id}")
        raise_on_status_errors(statuses)
        parent = parse_order_result(statuses[0], client_order_id, requested_quantity=order.quantity)
        self._client_coins[client_order_id] = (coin, is_spot)
        for status in statuses:
            if not isinstance(status, dict):
                continue
            detail = status.get("resting") or status.get("filled") or {}
            oid = detail.get("oid") if isinstance(detail, dict) else None
            if oid is None or oid == "":
                continue
            self._oid_clients[str(oid)] = (client_order_id, None, None)
        for suffix, reason in (
            (TP_CLOID_SUFFIX, FillReason.TAKE_PROFIT),
            (SL_CLOID_SUFFIX, FillReason.STOP_LOSS),
        ):
            self._child_parents[client_order_id_to_cloid(f"{client_order_id}:{suffix}")] = (
                client_order_id,
                reason,
                FillEntry.OUT,
            )
        return parent

    async def modify_order(self, modification: OrderModification) -> OrderResult:
        """Translate and submit an order modification, preferring cloid.

        Merges over the live record, submits ``modify_order`` by cloid,
        then re-queries ``orderStatus`` for the authoritative result.
        """
        exchange = self._require_exchange()
        current = await self._current_order_record(modification.client_order_id)
        coin, _ = await self._resolve_coin(modification.client_order_id)
        kwargs = build_modify_action(modification, coin=coin, current=current)
        kwargs["oid"] = Cloid(kwargs["oid"])
        kwargs["cloid"] = Cloid(kwargs["cloid"])
        response = await self._run_exchange(exchange.modify_order, **kwargs)
        self._check_action_ok(response, context=f"amending order {modification.client_order_id}")
        result = await self.get_order_by_client_id(modification.client_order_id)
        if result is None:
            raise OrderNotFoundError(
                f"Order {modification.client_order_id} was amended but could not be re-queried"
            )
        if result.platform_order_id is not None:
            self._oid_clients[result.platform_order_id] = (
                modification.client_order_id,
                None,
                None,
            )
        return result

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order via ``cancelByCloid``.

        Raises ``OrderNotFoundError`` if the venue reports the order was
        never placed.  A cancel that removes the order from the book reads
        back as ``unknownOid`` — reported as CANCELLED, since absence after
        a cancel ack means gone.  Cloids are not venue-unique: a reused
        client id leaves sibling orders working, and the re-query then
        truthfully reports the remainder as OPEN.
        """
        exchange = self._require_exchange()
        coin, _ = await self._resolve_coin(client_order_id)
        cancel_coin, raw_cloid = build_cancel_action(client_order_id, coin=coin)
        response = await self._run_exchange(exchange.cancel_by_cloid, cancel_coin, Cloid(raw_cloid))
        self._check_action_ok(response, context=f"cancelling order {client_order_id}")
        result = await self.get_order_by_client_id(client_order_id)
        if result is None:
            now = _utcnow()
            return OrderResult(
                client_order_id=client_order_id,
                platform_order_id=None,
                status=OrderStatus.CANCELLED,
                filled_quantity=Decimal("0"),
                average_fill_price=None,
                created_at=now,
                updated_at=now,
            )
        return result

    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None:
        """Query order status by cloid: open scan first, ``orderStatus`` second.

        Open orders are the live truth — ``orderStatus``-by-cloid resolves
        to the *original* leg after a modify (observed live: replacement
        live under a new oid while the query returns the cancelled
        original), so it is consulted only when no open entry carries the
        cloid.  Returns None on ``unknownOid``.  Average fill price is not
        reported by either endpoint and reads back as None — fills carry
        prices via fill events.
        """
        want = client_order_id_to_cloid(client_order_id)
        try:
            record = await self._current_order_record(client_order_id)
        except OrderNotFoundError:
            record = None
        if record is not None:
            return OrderResult(
                client_order_id=client_order_id,
                platform_order_id=record.platform_order_id,
                status=record.status,
                filled_quantity=record.filled_quantity,
                average_fill_price=record.average_fill_price,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
        exchange = self._require_exchange()
        response = await self._run_exchange(
            exchange.info.query_order_by_cloid, self._config.wallet_address, Cloid(want)
        )
        if not isinstance(response, dict):
            raise PlatformError(f"Unexpected orderStatus shape {response!r}")
        if response.get("status") == "unknownOid":
            return None
        try:
            payload = response["order"]
            order_object = payload["order"]
        except (KeyError, TypeError) as exc:
            raise PlatformError(f"Unexpected orderStatus shape {response!r}") from exc
        coin = str(order_object.get("coin") or "")
        if not coin:
            raise PlatformError(f"orderStatus entry is missing coin: {response!r}")
        instrument = from_hyperliquid_coin(
            coin, is_spot=self._is_spot_coin(coin), spot_pair_coins=self._reverse_pair_coins()
        )
        record = translate_order_entry(
            {
                **order_object,
                "status": payload.get("status"),
                "statusTimestamp": payload.get("statusTimestamp"),
            },
            instrument=instrument,
        )
        return OrderResult(
            client_order_id=client_order_id,
            platform_order_id=record.platform_order_id,
            status=record.status,
            filled_quantity=record.filled_quantity,
            average_fill_price=record.average_fill_price,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    # ---- Instrument metadata ----

    def _cached_spec(self, instrument: Instrument) -> InstrumentSpec | None:
        """Return the cached spec when fresh, else None (TTL-governed)."""
        cached = self._instrument_specs.get(instrument)
        if cached is None:
            return None
        spec, fetched_at = cached
        ttl = self._spec_ttl
        if ttl is None or time.monotonic() - fetched_at < ttl:
            return spec
        self._instrument_specs.pop(instrument, None)
        return None

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch (or return a cached) ``InstrumentSpec`` for ``instrument``.

        Perps read ``meta.universe`` (``szDecimals``/``maxLeverage``);
        spot reads ``spotMeta`` (base-token ``szDecimals``).  Tick is
        ``10^-(MAX_DECIMALS - szDecimals)`` with the 5-sig-fig rule enforced
        at placement, not here; lot is ``10^-szDecimals``; minimums are the
        venue $10 floors; maximum quantity is the tier-implied non-binding
        upper bound (real caps enforce at placement); delisted entries
        raise ``InvalidSymbolError``.
        """
        cached = self._cached_spec(instrument)
        if cached is not None:
            return cached
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(instrument)
        is_spot = instrument.asset_class == AssetClass.SPOT
        if not is_spot:
            meta = await self._run_exchange(exchange.info.meta)
            entry = next(
                (
                    e
                    for e in meta.get("universe") or []
                    if isinstance(e, dict) and e.get("name") == coin
                ),
                None,
            )
            if entry is None or entry.get("isDelisted"):
                raise InvalidSymbolError(f"Instrument {coin!r} is not tradable on Hyperliquid")
            sz_decimals = int(entry.get("szDecimals", 0))
            max_leverage = entry.get("maxLeverage")
            decimals_cap = MAX_DECIMALS_PERPS
        else:
            spot_meta = await self._run_exchange(exchange.info.spot_meta)
            alias = exchange.info.name_to_coin.get(coin, coin)
            entry = next(
                (
                    e
                    for e in spot_meta.get("universe") or []
                    if isinstance(e, dict) and e.get("name") == alias
                ),
                None,
            )
            if entry is None:
                raise InvalidSymbolError(f"Instrument {coin!r} is not tradable on Hyperliquid")
            token_ids = entry.get("tokens") or []
            token_rows = {
                t.get("index"): t for t in spot_meta.get("tokens") or [] if isinstance(t, dict)
            }
            base_row = token_rows.get(token_ids[0]) if len(token_ids) == 2 else None
            if base_row is None or base_row.get("szDecimals") is None:
                raise InvalidSymbolError(f"Spot pair {coin!r} has no usable token metadata")
            sz_decimals = int(base_row["szDecimals"])
            max_leverage = None
            decimals_cap = MAX_DECIMALS_SPOT
        tick_size = Decimal(1).scaleb(-(decimals_cap - sz_decimals))
        lot_size = Decimal(1).scaleb(-sz_decimals)
        tier = max_limit_notional(int(max_leverage) if max_leverage is not None else 1)
        spec = InstrumentSpec(
            tick_size=tick_size,
            lot_size=lot_size,
            min_qty=lot_size,
            max_qty=tier / tick_size,
            min_notional=Decimal("10"),
            price_precision=decimals_cap - sz_decimals,
            qty_precision=sz_decimals,
            max_leverage=Decimal(str(max_leverage)) if max_leverage is not None else None,
        )
        self._instrument_specs[instrument] = (spec, time.monotonic())
        return spec

    # ---- Capability reporting ----

    def supported_order_types(self) -> frozenset[OrderType]:
        """Return the supported order types — all four guaranteed types."""
        return frozenset(
            {
                OrderType.MARKET,
                OrderType.LIMIT,
                OrderType.STOP,
                OrderType.STOP_LIMIT,
            }
        )

    # ---- Rate limits ----

    async def get_rate_limits(self) -> RateLimits:
        """Return the live weight-budget state, not constants."""
        raise NotImplementedError

    # ---- Market data ----

    async def fetch_ticker(self, instrument: Instrument) -> Ticker | None:
        """Snapshot best bid/ask via ``l2Book`` plus mark from asset ctx.

        Returns None when the book is empty (no live quote); a malformed
        book raises.  ``last`` is not reported by these endpoints and stays
        None — mid derives via ``Ticker.mid``.
        """
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(instrument)
        is_spot = instrument.asset_class == AssetClass.SPOT
        book = await self._run_exchange(exchange.info.l2_snapshot, coin)
        try:
            levels = book["levels"]
            bids, asks = levels[0], levels[1]
            if not bids or not asks:
                return None
            best_bid = str(bids[0]["px"])
            best_ask = str(asks[0]["px"])
        except (KeyError, IndexError, TypeError) as exc:
            raise PlatformError(f"Unexpected l2Book shape for {coin}") from exc
        mark: str | None = None
        if not is_spot:
            meta_ctx = await self._run_exchange(exchange.info.meta_and_asset_ctxs)
            try:
                universe, ctxs = meta_ctx[0]["universe"], meta_ctx[1]
                index = next(
                    i
                    for i, e in enumerate(universe)
                    if isinstance(e, dict) and e.get("name") == coin
                )
                mark = str(ctxs[index].get("markPx"))
            except (StopIteration, KeyError, IndexError, TypeError) as exc:
                raise PlatformError(f"Unexpected asset ctx shape for {coin}") from exc
        else:
            # Spot ctxs key by the venue alias (``@107``), not the pair
            # spelling the canonical instrument carries.
            alias = exchange.info.name_to_coin.get(coin, coin)
            spot_ctx = await self._run_exchange(exchange.info.spot_meta_and_asset_ctxs)
            try:
                ctxs = spot_ctx[1]
                ctx = next(c for c in ctxs if isinstance(c, dict) and c.get("coin") == alias)
                mark = str(ctx.get("markPx"))
            except (StopIteration, KeyError, IndexError, TypeError) as exc:
                raise PlatformError(f"Unexpected spot ctx shape for {coin}") from exc
        return translate_ticker(None, best_bid=best_bid, best_ask=best_ask, mark=mark)

    # ---- Position TP/SL modification ----

    async def modify_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
        *,
        take_profit: TpSlAttachment | None = None,
        stop_loss: TpSlAttachment | None = None,
    ) -> None:
        """Modify TP/SL on an open position via the ``positionTpsl`` grouping."""
        raise NotImplementedError

    async def get_position_tpsl(
        self,
        instrument: Instrument,
        position_id: str,
    ) -> tuple[TpSlAttachment | None, TpSlAttachment | None] | None:
        """Read the current TP/SL on an open position."""
        raise NotImplementedError

    # ---- Reconciliation data ----

    @staticmethod
    def _state_time(state: Any, field: str = "time") -> datetime:
        """Decode a state millisecond timestamp, defaulting to now."""
        try:
            ms = int(str(state.get(field)))
        except (AttributeError, TypeError, ValueError):
            return _utcnow()
        seconds, millis = divmod(ms, 1000)
        return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=millis * 1000)

    async def fetch_positions(self) -> list[Position]:
        """Fetch open legs from ``clearinghouseState`` (spot has no legs).

        Flat (zero-size) legs are skipped; malformed legs are logged and
        skipped, never aborting the snapshot.
        """
        exchange = self._require_exchange()
        state = await self._run_exchange(exchange.info.user_state, self._config.wallet_address)
        timestamp = self._state_time(state)
        result: list[Position] = []
        legs = state.get("assetPositions") if isinstance(state, dict) else None
        for leg in legs or []:
            try:
                coin = str((leg.get("position") or {}).get("coin") or "")
                instrument = from_hyperliquid_coin(coin, is_spot=False)
                position = translate_position(leg, instrument=instrument, timestamp=timestamp)
            except Exception:
                logger.exception("Skipping malformed position leg: %s", leg)
                continue
            if position.quantity != 0:
                result.append(position)
        return result

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch per-currency balances from ``spotClearinghouseState``."""
        exchange = self._require_exchange()
        state = await self._run_exchange(exchange.info.spot_user_state, self._config.wallet_address)
        timestamp = _utcnow()
        result: dict[str, Balance] = {}
        rows = state.get("balances") if isinstance(state, dict) else None
        for row in rows or []:
            try:
                currency = str(row.get("coin") or "")
                if not currency:
                    continue
                result[currency] = translate_balance(row, currency=currency, timestamp=timestamp)
            except Exception:
                logger.exception("Skipping malformed balance row: %s", row)
                continue
        return result

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch every open order, keyed by client order id.

        Derived TP/SL child legs are attachments of their parent in the
        unified model, not orders of their own, so they are excluded — the
        parent is reported, keyed by client id.  Venue-created legs we never
        minted a cloid for key by platform oid; entries with neither id are
        skipped, never collapsed onto an empty key.
        """
        result: dict[str, OrderRecord] = {}
        cloid_to_client: dict[str, str] = {}
        child_cloids: set[str] = set()
        for client_order_id in list(self._client_coins):
            cloid_to_client[client_order_id_to_cloid(client_order_id)] = client_order_id
            child_cloids.update(
                client_order_id_to_cloid(f"{client_order_id}:{suffix}")
                for suffix in (TP_CLOID_SUFFIX, SL_CLOID_SUFFIX)
            )
        child_cloids.update(self._child_parents)
        for entry in await self._open_order_entries():
            if not isinstance(entry, dict):
                continue
            if str(entry.get("cloid") or "") in child_cloids:
                continue
            try:
                coin = str(entry.get("coin") or "")
                instrument = from_hyperliquid_coin(
                    coin,
                    is_spot=self._is_spot_coin(coin),
                    spot_pair_coins=self._reverse_pair_coins(),
                )
                order = translate_order_entry(entry, instrument=instrument)
            except Exception:
                logger.exception("Skipping malformed open order entry: %s", entry)
                continue
            key: str | None = cloid_to_client.get(order.client_order_id, order.client_order_id)
            if not key:
                key = order.platform_order_id
            if not key:
                logger.error("Open order entry has no order id: %s", entry)
                continue
            result[key] = order
        return result

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent fills, grouped by client order id.

        Without ``since`` reads the recent window (``userFills``, ≤2000);
        with ``since`` reads ``userFillsByTime`` from that bound (server
        filters; a client-side guard holds the boundary).  Entries dedupe
        on ``(hash, tid)``; oid-attributed fills key by client id (TP/SL
        children by parent with their reason), unknown oids key by raw oid.
        """
        await self._refresh_oid_index()
        exchange = self._require_exchange()
        if since is not None:
            if since.tzinfo is None:
                raise ValueError("since must be timezone-aware (UTC)")
            entries = await self._run_exchange(
                exchange.info.user_fills_by_time,
                self._config.wallet_address,
                int(since.timestamp() * 1000),
            )
        else:
            entries = await self._run_exchange(
                exchange.info.user_fills, self._config.wallet_address
            )
        if not isinstance(entries, list):
            raise PlatformError(f"Unexpected fills shape {entries!r}")
        result: dict[str, list[FillRecord]] = {}
        seen: set[tuple[str, str]] = set()
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            raw_hash, raw_tid = entry.get("hash"), entry.get("tid")
            if raw_hash in (None, "") or raw_tid in (None, ""):
                continue
            key = (str(raw_hash), str(raw_tid))
            if key in seen:
                continue
            seen.add(key)
            oid = str(entry.get("oid") or "")
            attributed = self._oid_clients.get(oid)
            if attributed is not None:
                client_order_id, reason, entry_side = attributed
            else:
                client_order_id, reason, entry_side = (oid or key[0]), None, None
            try:
                coin = str(entry.get("coin") or "")
                instrument = from_hyperliquid_coin(
                    coin,
                    is_spot=self._is_spot_coin(coin),
                    spot_pair_coins=self._reverse_pair_coins(),
                )
                fill = translate_fill(
                    entry,
                    instrument=instrument,
                    client_order_id=client_order_id,
                    reason=reason,
                    fill_entry=entry_side,
                )
            except Exception:
                logger.exception("Skipping malformed fill entry: %s", entry)
                continue
            if since is not None and fill.fill_timestamp < since:
                continue
            result.setdefault(client_order_id, []).append(fill)
        return result

    # ---- Leverage + margin intent ----

    async def _require_store(self) -> StateStore:
        if self._state_store is None:
            raise PlatformError(
                "HyperliquidAdapter was constructed without a state_store — "
                "leverage/margin intent persistence is unavailable"
            )
        return self._state_store

    async def _venue_leverage(self, coin: str) -> tuple[int, bool] | None:
        """Read live ``(leverage, is_cross)`` for a coin; None with no open leg."""
        exchange = self._require_exchange()
        state = await self._run_exchange(exchange.info.user_state, self._config.wallet_address)
        legs = state.get("assetPositions") if isinstance(state, dict) else None
        for leg in legs or []:
            position = leg.get("position") if isinstance(leg, dict) else None
            if not isinstance(position, dict) or position.get("coin") != coin:
                continue
            leverage = position.get("leverage") or {}
            try:
                return int(leverage.get("value", 1)), leverage.get("type") == "cross"
            except (TypeError, ValueError):
                return None
        return None

    async def _tier_max_leverage(self, coin: str) -> int:
        """Effective max leverage: universe max capped by the zero-position tier."""
        exchange = self._require_exchange()
        meta = await self._run_exchange(exchange.info.meta)
        universe = meta.get("universe") if isinstance(meta, dict) else None
        entry = next(
            (e for e in universe or [] if isinstance(e, dict) and e.get("name") == coin),
            None,
        )
        if entry is None:
            raise InvalidSymbolError(f"Unknown coin {coin!r} on Hyperliquid")
        try:
            universe_max = int(entry.get("maxLeverage") or 1)
        except (TypeError, ValueError):
            universe_max = 1
        tables = meta.get("marginTables") if isinstance(meta, dict) else None
        if isinstance(tables, list):
            for table in tables:
                if (
                    not isinstance(table, list)
                    or len(table) != 2
                    or table[0] != entry.get("marginTableId")
                ):
                    continue
                tiers = table[1].get("marginTiers") if isinstance(table[1], dict) else None
                for tier in tiers or []:
                    if not isinstance(tier, dict):
                        continue
                    try:
                        if Decimal(str(tier.get("lowerBound", "0"))) != 0:
                            continue
                        return min(universe_max, int(tier.get("maxLeverage") or universe_max))
                    except (TypeError, ValueError):
                        continue
        return universe_max

    async def _stored_leverage(self, coin: str) -> int | None:
        if self._state_store is None:
            return None
        raw = await self._state_store.get_adapter_config(f"leverage:{coin}")
        try:
            return int(raw) if raw is not None else None
        except ValueError:
            return None

    async def _stored_margin_mode(self, coin: str) -> MarginMode | None:
        if self._state_store is None:
            return None
        raw = await self._state_store.get_adapter_config(f"margin_mode:{coin}")
        try:
            return MarginMode(raw) if raw is not None else None
        except ValueError:
            return None

    async def _resolved_is_cross(self, coin: str) -> bool:
        """Mode for an ``updateLeverage`` call: stored intent, else venue, else default."""
        stored = await self._stored_margin_mode(coin)
        if stored is not None:
            return stored is MarginMode.CROSS
        venue = await self._venue_leverage(coin)
        if venue is not None:
            return venue[1]
        return self._config.default_margin_mode is MarginMode.CROSS

    async def _resolved_leverage(self, coin: str) -> int:
        """Leverage for an ``updateLeverage`` call: stored intent, else venue, else default."""
        stored = await self._stored_leverage(coin)
        if stored is not None:
            return stored
        venue = await self._venue_leverage(coin)
        if venue is not None:
            return venue[0]
        return self._config.default_leverage

    async def _submit_leverage(self, coin: str, leverage: int, is_cross: bool) -> None:
        exchange = self._require_exchange()
        response = await self._run_exchange(exchange.update_leverage, leverage, coin, is_cross)
        self._check_action_ok(response, context=f"setting leverage for {coin}")

    async def set_leverage(
        self,
        instrument: Instrument,
        *,
        leverage: int = 1,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset leverage via ``updateLeverage`` and persist intent.

        Owns the leverage number only — the mode resolves stored intent,
        else venue, else default.  Above the tier cap raises
        ``InvalidOrderError`` (never clamped); spot raises
        ``InvalidSymbolError``.  Intent persists only after the venue
        accepts.
        """
        raw_leverage: Any = leverage
        if isinstance(raw_leverage, bool) or not isinstance(raw_leverage, int) or raw_leverage < 1:
            raise InvalidOrderError(f"leverage must be an integer >= 1, got {leverage}")
        if on_drift not in ("reapply", "notify", "halt"):
            raise ValueError(f"on_drift must be reapply/notify/halt, got {on_drift}")
        if instrument.asset_class == AssetClass.SPOT:
            raise InvalidSymbolError(f"Spot instrument {instrument.symbol} has no leverage")
        coin = to_hyperliquid_coin(instrument)
        cap = await self._tier_max_leverage(coin)
        if leverage > cap:
            raise InvalidOrderError(f"Leverage {leverage} exceeds max {cap} for {coin}")
        await self._submit_leverage(coin, leverage, await self._resolved_is_cross(coin))
        store = await self._require_store()
        await store.set_adapter_config(f"leverage:{coin}", str(leverage))
        await store.set_adapter_config(f"leverage.on_drift:{coin}", on_drift)
        await store.set_adapter_config(
            f"leverage.auto_apply:{coin}", "1" if auto_apply_on_connect else "0"
        )

    async def get_leverage(self, instrument: Instrument) -> tuple[int, bool] | None:
        """Query per-asset ``(leverage, is_cross)``; None with no open leg."""
        if instrument.asset_class == AssetClass.SPOT:
            return None
        return await self._venue_leverage(to_hyperliquid_coin(instrument))

    async def remove_leverage(self, instrument: Instrument) -> None:
        """Drop stored per-asset leverage intent (venue untouched)."""
        store = await self._require_store()
        coin = to_hyperliquid_coin(instrument)
        await store.delete_adapter_config(f"leverage:{coin}")
        await store.delete_adapter_config(f"leverage.on_drift:{coin}")
        await store.delete_adapter_config(f"leverage.auto_apply:{coin}")

    async def top_up_isolated_margin(self, instrument: Instrument, *, amount_usdc: Decimal) -> None:
        """Add (positive) or remove (negative) isolated margin by delta.

        Goes through ``update_isolated_margin`` — the SDK wraps no
        leverage-targeted top-up, so targeting a leverage number is out of
        scope; strict-isolated removal refusals surface as mapped venue
        errors.  ``isBuy`` is sent ``true``: a documented venue no-op.
        """
        if instrument.asset_class == AssetClass.SPOT:
            raise InvalidSymbolError(f"Spot instrument {instrument.symbol} has no isolated margin")
        exchange = self._require_exchange()
        coin = to_hyperliquid_coin(instrument)
        response = await self._run_exchange(
            exchange.update_isolated_margin, float(amount_usdc), coin
        )
        self._check_action_ok(response, context=f"topping up isolated margin for {coin}")

    async def set_margin_mode(
        self,
        instrument: Instrument,
        mode: MarginMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset margin mode via ``updateLeverage`` and persist intent.

        Owns the mode only — leverage is preserved (stored, else venue,
        else default).  ``mode`` is the enum or ``"cross"``/``"isolated"``.
        """
        try:
            resolved = MarginMode(mode)
        except ValueError:
            raise ValueError(
                f"mode must be one of {[m.value for m in MarginMode]}, got {mode!r}"
            ) from None
        if on_drift not in ("reapply", "notify", "halt"):
            raise ValueError(f"on_drift must be reapply/notify/halt, got {on_drift}")
        if instrument.asset_class == AssetClass.SPOT:
            raise InvalidSymbolError(f"Spot instrument {instrument.symbol} has no margin mode")
        coin = to_hyperliquid_coin(instrument)
        await self._submit_leverage(
            coin, await self._resolved_leverage(coin), resolved is MarginMode.CROSS
        )
        store = await self._require_store()
        await store.set_adapter_config(f"margin_mode:{coin}", resolved.value)
        await store.set_adapter_config(f"margin_mode.on_drift:{coin}", on_drift)
        await store.set_adapter_config(
            f"margin_mode.auto_apply:{coin}", "1" if auto_apply_on_connect else "0"
        )

    async def get_margin_mode(self, instrument: Instrument) -> MarginMode | None:
        """Query the per-asset margin mode; None with no open leg (or spot)."""
        if instrument.asset_class == AssetClass.SPOT:
            return None
        venue = await self._venue_leverage(to_hyperliquid_coin(instrument))
        if venue is None:
            return None
        return MarginMode.CROSS if venue[1] else MarginMode.ISOLATED

    async def remove_margin_mode(self, instrument: Instrument) -> None:
        """Drop stored per-asset margin-mode intent (venue untouched)."""
        store = await self._require_store()
        coin = to_hyperliquid_coin(instrument)
        await store.delete_adapter_config(f"margin_mode:{coin}")
        await store.delete_adapter_config(f"margin_mode.on_drift:{coin}")
        await store.delete_adapter_config(f"margin_mode.auto_apply:{coin}")

    def _decode_lev_intent_key(self, key: str) -> str | None:
        """Extract the coin from a stored ``leverage:{coin}`` key (None for policy rows)."""
        if not key.startswith("leverage:") or "." in key:
            return None
        return key.removeprefix("leverage:")

    def _decode_mode_intent_key(self, key: str) -> str | None:
        """Extract the coin from a stored ``margin_mode:{coin}`` key (None for policy rows)."""
        if not key.startswith("margin_mode:") or "." in key:
            return None
        return key.removeprefix("margin_mode:")

    async def _halt_for_drift(self, coin: str, *, reason: str, detail: str) -> None:
        """Enter an instrument halt for drift, degrading to a log without setup."""
        if self._halt_machine is None:
            logger.warning("Cannot enter %s halt for %s — no halt machine attached", reason, coin)
            return
        try:
            instrument = from_hyperliquid_coin(
                coin,
                is_spot=self._is_spot_coin(coin),
                spot_pair_coins=self._reverse_pair_coins(),
            )
        except Exception:
            logger.exception("Cannot resolve instrument for %s halt on %s", reason, coin)
            return
        self._halt_machine.enter_halt(
            scope="instrument", instrument=instrument, reason=reason, detail=detail
        )

    async def _reconcile_leverage_row(self, coin: str, stored: int) -> None:
        venue = await self._venue_leverage(coin)
        if venue is None:
            return
        if venue[0] == stored:
            return
        policy = None
        if self._state_store is not None:
            policy = await self._state_store.get_adapter_config(f"leverage.on_drift:{coin}")
        on_drift = policy or "reapply"
        detail = f"stored={stored} venue={venue[0]}"
        if on_drift == "reapply":
            await self._submit_leverage(coin, stored, await self._resolved_is_cross(coin))
        elif on_drift == "notify":
            logger.warning("Leverage drift on %s: %s", coin, detail)
        else:
            await self._halt_for_drift(coin, reason="leverage_drift", detail=detail)

    async def _reconcile_margin_row(self, coin: str, stored: MarginMode) -> None:
        venue = await self._venue_leverage(coin)
        if venue is None:
            return
        if (venue[1] and stored is MarginMode.CROSS) or (
            not venue[1] and stored is MarginMode.ISOLATED
        ):
            return
        policy = None
        if self._state_store is not None:
            policy = await self._state_store.get_adapter_config(f"margin_mode.on_drift:{coin}")
        on_drift = policy or "reapply"
        detail = f"stored={stored.value} venue={'cross' if venue[1] else 'isolated'}"
        if on_drift == "reapply":
            await self._submit_leverage(
                coin, await self._resolved_leverage(coin), stored is MarginMode.CROSS
            )
        elif on_drift == "notify":
            logger.warning("Margin mode drift on %s: %s", coin, detail)
        else:
            await self._halt_for_drift(coin, reason="margin_mode_drift", detail=detail)

    async def reconcile_user_intent(self) -> None:
        """Reconcile stored per-asset leverage/mode intent with the venue.

        Each drifted coin executes its own stored policy; failures are
        logged per coin without aborting the pass.  No position-mode
        reconciliation — one-way is asserted, not managed.
        """
        if self._state_store is None:
            return
        for key, value in (await self._state_store.list_adapter_config("leverage:")).items():
            coin = self._decode_lev_intent_key(key)
            if coin is None:
                continue
            try:
                stored = int(value)
            except ValueError:
                continue
            try:
                await self._reconcile_leverage_row(coin, stored)
            except Exception:
                logger.exception("Leverage reconcile failed for %s", coin)
        for key, value in (await self._state_store.list_adapter_config("margin_mode:")).items():
            coin = self._decode_mode_intent_key(key)
            if coin is None:
                continue
            try:
                stored_mode = MarginMode(value)
            except ValueError:
                continue
            try:
                await self._reconcile_margin_row(coin, stored_mode)
            except Exception:
                logger.exception("Margin mode reconcile failed for %s", coin)

    async def _reapply_stored_intent(self) -> None:
        """Impose auto-apply stored intent after connect; failures never break connect."""
        if self._state_store is None:
            return
        for key, value in (await self._state_store.list_adapter_config("leverage:")).items():
            coin = self._decode_lev_intent_key(key)
            if coin is None:
                continue
            try:
                if await self._state_store.get_adapter_config(f"leverage.auto_apply:{coin}") == "0":
                    continue
                await self._submit_leverage(coin, int(value), await self._resolved_is_cross(coin))
            except Exception:
                logger.exception("Leverage reapply failed for %s on connect", coin)
        for key, value in (await self._state_store.list_adapter_config("margin_mode:")).items():
            coin = self._decode_mode_intent_key(key)
            if coin is None:
                continue
            try:
                if (
                    await self._state_store.get_adapter_config(f"margin_mode.auto_apply:{coin}")
                    == "0"
                ):
                    continue
                await self._submit_leverage(
                    coin, await self._resolved_leverage(coin), MarginMode(value) is MarginMode.CROSS
                )
            except Exception:
                logger.exception("Margin mode reapply failed for %s on connect", coin)
