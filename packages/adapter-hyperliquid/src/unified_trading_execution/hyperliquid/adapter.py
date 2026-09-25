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
    build_cancel_action,
    build_modify_action,
    build_place_order_action,
    client_order_id_to_cloid,
    max_limit_notional,
    max_market_notional,
    parse_order_result,
    quantize_price,
    round_price_to_tick,
    validate_size,
)
from unified_trading_execution.hyperliquid.signing import (
    assert_user_role_for_signing,
    build_wallet,
)
from unified_trading_execution.hyperliquid.streams import translate_order_entry
from unified_trading_execution.hyperliquid.symbols import (
    from_hyperliquid_coin,
    to_hyperliquid_coin,
)
from unified_trading_execution.state.halt import HaltStateMachine
from unified_trading_execution.state.store import StateStore
from unified_trading_execution.types.enums import AssetClass, OrderSide, OrderStatus, OrderType
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

    async def _max_leverage_for(self, coin: str) -> int:
        """Read the live universe ``maxLeverage`` for tier-cap checks.

        One ``meta`` call per order for now; repoint at cached
        ``InstrumentSpec.max_leverage`` once spec caching exists.
        """
        exchange = self._require_exchange()
        meta = await self._run_exchange(exchange.info.meta)
        for entry in meta.get("universe") or []:
            if isinstance(entry, dict) and entry.get("name") == coin:
                try:
                    return int(entry.get("maxLeverage") or 1)
                except (TypeError, ValueError):
                    return 1
        raise InvalidSymbolError(f"Unknown coin {coin!r} in meta")

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
        try:
            asset = exchange.info.name_to_asset(coin)
            sz_decimals = int(exchange.info.asset_to_sz_decimals[asset])
        except KeyError as exc:
            raise InvalidSymbolError(f"Unknown coin {coin!r} on Hyperliquid") from exc

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
            if not is_spot:
                cap = max_market_notional(await self._max_leverage_for(coin))
                if market_limit_price * order.quantity > cap:
                    raise InvalidOrderError(f"Market notional exceeds tier cap {cap} for {coin}")
        elif order.order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and not is_spot:
            assert order.price is not None
            cap = max_limit_notional(await self._max_leverage_for(coin))
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
        parent = parse_order_result(statuses[0], client_order_id, requested_quantity=order.quantity)
        self._client_coins[client_order_id] = (coin, is_spot)
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
        return result

    async def cancel_order(self, client_order_id: str) -> OrderResult:
        """Cancel an existing order via ``cancelByCloid``.

        Raises ``OrderNotFoundError`` if the venue reports the order was
        never placed.  A cancel that removes the order from the book reads
        back as ``unknownOid`` — reported as CANCELLED, since absence after
        a cancel ack means gone.
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

    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec:
        """Fetch trading rules for an instrument from ``meta``/``spotMeta``.

        Raises ``InvalidSymbolError`` if the instrument is not tradable.
        """
        raise NotImplementedError

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
        """Snapshot bid/ask/mid via ``allMids`` + ``l2Book``.

        Weight-2 info calls, ~1s cache.  Returns None when the book is empty.
        """
        raise NotImplementedError

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

    async def fetch_positions(self) -> list[Position]:
        """Fetch open legs from ``clearinghouseState``."""
        raise NotImplementedError

    async def fetch_balances(self) -> dict[str, Balance]:
        """Fetch per-currency balances from ``spotClearinghouseState``."""
        raise NotImplementedError

    async def fetch_open_orders(self) -> dict[str, OrderRecord]:
        """Fetch open orders (``openOrders`` + ``frontendOpenOrders``) keyed by cloid."""
        raise NotImplementedError

    async def fetch_fills(self, *, since: datetime | None = None) -> dict[str, list[FillRecord]]:
        """Fetch recent fills (``userFills`` + ``userFillsByTime`` merged and deduped)."""
        raise NotImplementedError

    # ---- Leverage + margin intent ----

    async def set_leverage(
        self,
        instrument: Instrument,
        *,
        leverage: int = 1,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset leverage via ``updateLeverage`` and persist intent.

        Owns the leverage number only — the margin mode comes from the
        separate margin-mode intent (stored intent, else live venue value,
        else the configured default), so each field has exactly one writer.
        Max is checked against both ``universe.maxLeverage`` and the
        ``marginTables`` tiers — block with ``InvalidOrderError``, never
        silently clamp.
        """
        raise NotImplementedError

    async def get_leverage(self, instrument: Instrument) -> tuple[int, bool] | None:
        """Query per-asset ``(leverage, is_cross)`` from the venue."""
        raise NotImplementedError

    async def remove_leverage(self, instrument: Instrument) -> None:
        """Drop stored per-asset leverage intent (venue untouched)."""
        raise NotImplementedError

    async def top_up_isolated_margin(self, instrument: Instrument, *, amount_usdc: Decimal) -> None:
        """Top up isolated margin via ``updateIsolatedMargin`` semantics.

        ``isBuy`` is passed as ``true`` — a documented venue no-op until
        Hyperliquid hedge mode exists.
        """
        raise NotImplementedError

    async def set_margin_mode(
        self,
        instrument: Instrument,
        mode: MarginMode | str,
        *,
        on_drift: Literal["reapply", "notify", "halt"] = "reapply",
        auto_apply_on_connect: bool = True,
    ) -> None:
        """Set per-asset margin mode via ``updateLeverage`` and persist intent.

        Owns the mode only — leverage is preserved (stored leverage intent,
        else live venue value, else the configured default).  ``mode`` is
        the ``MarginMode`` enum or the raw strings ``"cross"`` /
        ``"isolated"``.  Each intent carries its own drift policy, so mode
        drift and leverage drift are managed independently.
        """
        raise NotImplementedError

    async def get_margin_mode(self, instrument: Instrument) -> MarginMode | None:
        """Query the per-asset margin mode from the venue for an instrument."""
        raise NotImplementedError

    async def remove_margin_mode(self, instrument: Instrument) -> None:
        """Drop stored per-asset margin-mode intent (venue untouched)."""
        raise NotImplementedError

    async def reconcile_user_intent(self) -> None:
        """Reconcile stored per-asset leverage/mode intent with the venue.

        Re-applies drifted leverage/mode per the stored on-drift policy.
        No position-mode reconciliation — one-way is asserted, not managed.
        """
        raise NotImplementedError
