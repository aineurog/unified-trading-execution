# Unified Trading Execution — Requirements Document

**Status:** Final — for development handover
**Project name:** Unified Trading Execution
**PyPI package (core):** `unified-trading-execution`
**Python import:** `import unified_trading_execution as ute`
**License:** Apache License 2.0

> **Note to any coding agent working from this document:** This document is the single source of truth for this project's architecture. Do not deviate from, reinterpret, simplify, or silently "improve" any decision recorded here — including type shapes, invariants, naming, scope boundaries (v1 vs. v2), and the layering rules in Section 4. If something here appears ambiguous, incomplete, or contradictory, do not resolve it unilaterally: stop and ask before writing code against it. Any new dependency, library, abstraction, or design pattern not named in this document must be proposed and approved before it is added — do not introduce something (e.g., a new data-processing library, a new architectural layer, a new external service) because it seems like good practice in isolation. Every deviation, however small, must be surfaced explicitly rather than folded in quietly.

---

## 1. Purpose

A modular, plug-and-play **order execution library** for Python, providing a single, consistent interface for placing, managing, and tracking orders across multiple trading platforms — crypto exchanges, retail/institutional brokers (MT5, cTrader, IBKR), and additional platforms added over time across all asset classes (spot, futures, forex, CFDs, stocks, options, and others).

The library is released in two generations:
- **v1** — the open-source core and its first two platform integrations (Bybit, cTrader).
- **v2** — additional platform adapters and additional feature modules, built strictly on top of v1's interfaces (see Section 13).

Guiding principles governing every decision in this document:

- **Single source of truth**: any given piece of logic (retries, risk rules, symbol handling, error handling) exists in exactly one place, shared across every platform — never duplicated per platform.
- **Adapter as translator, not decision-maker**: platform-specific code only converts between platform-native and unified representations. It contains no business logic, no retry policy, no risk decisions.
- **The platform is always the source of truth for account state.** The engine mirrors and reconciles against it; it never invents or overrides platform state.
- **Fail loud, not silent**: wherever the engine cannot guarantee correctness (unsupported feature, state mismatch, ambiguous retry outcome), it raises a clear, typed error or halts — it never guesses or fakes behavior.
- **v1 is a permanent foundation, not a stripped-down preview.** v2 is purely additive (Section 13) — v1 is never rewritten to accommodate v2 features.
- **Complexity budget**: every feature is deliberately placed in v1 core, deferred to v2, or left as the integrating user's own responsibility — the engine does not attempt to do everything.

---

## 2. Naming Conventions

- **PyPI package (core):** `unified-trading-execution`
- **PyPI packages (adapters):** `unified-trading-execution-bybit`, `unified-trading-execution-ctrader`, and (v2) `unified-trading-execution-mt5`, `unified-trading-execution-ibkr`, etc. — each installable independently so integrators only pull in the dependencies for platforms they actually use.
- **Python import namespace:** all packages share a single unified import namespace regardless of how many separate packages are installed, using a namespace-package pattern (PEP 420) — e.g.:
  ```python
  import unified_trading_execution as ute
  from unified_trading_execution.bybit import BybitAdapter
  from unified_trading_execution.ctrader import CTraderAdapter
  ```
  This mirrors the pattern used by projects like Apache Airflow's providers and OpenTelemetry's exporters/instrumentations — many independently installable packages, one coherent import surface. The short alias `ute` is a documentation/convention choice (same pattern as `numpy as np`), not baked into the package name itself.
- **GitHub repository:** `unified-trading-execution` (hyphenated, standard GitHub convention).
- **Convention going forward:** confirm exact availability of the above on PyPI and GitHub before first publish — this document assumes them as the target names.

---

## 3. Usage Model

The engine is a **Python library**, imported directly into the user's own application, bot, or script — not a standalone service in v1.

- The user creates engine/adapter instances that live as long as their own host process runs. Since most trading applications run continuously, the engine is expected to hold persistent connections and run continuously within that process — being "embedded" does not mean "short-lived."
- A standalone service/daemon mode (multiple independent clients sharing one running engine instance, potentially across machines) is explicitly out of scope for v1 and deferred to v2 (Section 5), to be built as a network-facing wrapper around the same core — not a redesign.

Both **async and sync APIs** are provided:
- The engine's actual implementation is async-native — all I/O (REST calls, websocket streams) is non-blocking, built on `asyncio`.
- The sync API is a thin facade over the same async core: a single persistent background event loop is created once per instance at construction time; each sync method call submits work to that loop (via `asyncio.run_coroutine_threadsafe` or equivalent) and blocks the calling thread until it completes. **Do not** implement the sync API by calling `asyncio.run()` per method — this creates and tears down a new event loop on every call, breaking connection reuse and severely harming performance.
- Both APIs share the same underlying implementation, state, and connections — there are never two divergent codepaths to maintain.

---

## 4. Architectural Layering

Every component belongs to exactly one of three layers. This discipline is a hard requirement, not a guideline, and applies to all future additions (v1 or v2):

- **Core** (platform-agnostic): unified data types, order dispatch orchestration, the risk-check chain, the state mirror and reconciliation logic, the idempotency mechanism, the internal event bus, the common exception hierarchy, history/query accessors. Core code never imports or references a specific platform.
- **Adapter** (one per platform): connection lifecycle (connect/disconnect/reconnect/heartbeat), translation between unified types and platform-native representations, translation of platform-native errors into the unified exception hierarchy, declaration of supported order types/capabilities. Contains no business logic, no retry policy, no risk decisions.
- **Configuration** (runtime data owned by neither core nor adapter code): credentials, broker-specific symbol alias tables (MT5), exchange/routing preferences (IBKR), testnet/live switches, storage location. Supplied by the user at construction time; never hardcoded or parsed by the engine.

**Decision rule for any future addition:** Does it need to know which platform it's talking to? → Adapter. Should it behave identically regardless of platform? → Core. Is it just a value that varies by account/broker with no logic attached? → Configuration.

This rule must be mechanically enforced, not just documented — see Section 14 (`import-linter` requirement).

---

## 5. v1 Scope

**Built in v1:**
- Order execution: place, modify, cancel, query status — normalized across platforms.
- Internal state mirror: positions and balances, updated in real time via each platform's websocket/event stream, reconciled periodically and on reconnect against the platform's own REST state (Section 6).
- Pre-trade risk-check chain: a pluggable, ordered sequence of validators run before every order dispatch (Section 7).
- Instrument metadata handling: tick size, lot size, precision, min/max quantity, min notional — fetched once per instrument and cached (Section 8).
- Two adapters: **Bybit** (crypto) and **cTrader** (broker), both built and tested against their sandbox/testnet environments.
- Idempotent order submission via client-generated order IDs (Section 9).
- A common, typed exception hierarchy translated by every adapter (Section 9).
- An internal event bus, exercised in v1 by the state mirror, and designed explicitly as the extension point future modules attach to without modifying core (Section 13).
- Structured (JSON) logging and a durable, queryable audit trail (Section 10).
- Read-only history/query accessors over that audit trail (Section 10.2).
- A documentation site (Section 12) and a full test suite (Section 11).

**Explicitly out of scope for v1, deferred to v2 (Section 13 defines how v2 attaches without rewriting v1):**
- A full stateful risk engine (portfolio-level exposure limits, drawdown/kill-switch, correlation risk).
- Synthetic/client-side order types (trailing stop, chandelier exit, or any order type not natively executed by the platform itself). The v1 user builds these themselves on top of core primitives (`place_order`/`modify_order`/`cancel_order` plus their own injected price feed and the event bus), since their own host process is already running continuously.
- MT5 and IBKR adapters (and any further platform adapters beyond Bybit/cTrader).
- Standalone service/daemon mode.
- Compiled-extension IP protection of any module.
- Multi-leg/combo instruments (e.g., option spreads) — a known future extension to the `Instrument` object, not built in v1.
- User-facing on/off toggles for individual risk-check validators (v1 ships thresholds-configurable, always-on validators — Section 7).
- Computed statistics/analytics (P&L, win rate, drawdown curves, etc.) — Section 10.2 explains why this is separated from raw history.

---

## 6. State Management

### 6.1 Core mechanism
- The platform is always authoritative for balances and positions. The engine never treats its own data as overriding the platform's.
- The engine maintains a **local mirror** per adapter instance, updated in real time from the platform's websocket/event stream (fills, position changes, balance changes) — not by polling.
- A periodic reconciliation pass compares the local mirror against a fresh REST pull from the platform. Reconciliation is also triggered immediately after any reconnect, since that is the highest-risk window for drift.

### 6.2 Storage: location, configurability, visibility
- Local mirror storage sits behind a `StateStore` interface. **SQLite** (via `aiosqlite`) is the default v1 implementation — file-based, zero external infrastructure required, sufficient since a single engine instance is the only writer to its own mirror. The interface must allow swapping in other backends later (e.g., Postgres/Redis for v2 multi-instance scenarios) without any change to core logic.
- **Storage location is a required constructor parameter with a sensible default, never hardcoded.** If the user does not supply a path, the default is a `./<project>_data/` directory relative to the process working directory, with an auto-generated filename encoding platform, market type, and account identifier (e.g. `./ute_data/bybit_futures_acct123.db`) — predictable and human-inspectable. The engine must never default to writing to a hidden/system-level location the user hasn't explicitly chosen or been told about.
- **The database file is intentionally not hidden from the user.** It is a plain, directly-inspectable SQLite file (openable with any standard SQLite browser), and the resolved path must be readable at runtime through the engine object itself (e.g. `engine.state_store.path`) so the integrator's own tooling (backups, monitoring, inspection) can locate and use it programmatically.
- The `state/` module must include a schema **migrations mechanism** from the start (versioned SQL files or a lightweight migration tool) — the schema will evolve (new audit fields, halt-state tracking, etc.) and must never require hand-patching a live database.

### 6.3 Reconciliation mismatch cases and required handling

| Case | Cause | Required handling |
|---|---|---|
| Position quantity mismatch | Missed fill/position event | Overwrite local mirror from platform value; halt new (exposure-increasing) orders on that instrument; log full discrepancy detail (old value, new value, timestamp of last known-good state) to the audit trail |
| Balance mismatch | Missed balance-update event | Overwrite from platform; halt new orders **account-wide** (balance has no per-instrument meaning); log |
| Orphan order on platform (unknown to local mirror) | Order placed outside engine tracking, or a retry edge case | Auto-import into local mirror as tracked; log as anomaly for review |
| Orphan order in local mirror (not on platform) | Missed cancel/reject event, or order never actually reached the platform | Remove from local mirror; log discrepancy |
| Partial fill discrepancy | Missed fill event, scoped to a single order | Correct fill amount from platform; recalculate the affected position (a specific instance of the position-mismatch case above); log |

### 6.4 Halt mechanism — full specification

Halting is an explicit state machine, not a passive wait condition: `ACTIVE → HALTED → CLEARED`.

- **Trigger for entry**: any reconciliation mismatch per the table above.
- **What is blocked while halted**: only orders that would *open or increase* exposure on the affected instrument (or account, for a balance mismatch), rejected immediately by the risk-check chain with a specific typed exception (`InstrumentHaltedError` / `AccountHaltedError`) — not queued, not delayed.
- **What is never blocked while halted**: reduce-only/closing orders, order cancellation, and all read operations (position/balance/order-status queries, history accessors). A mismatch must never trap a user in a position they cannot exit.
- **Clearing**:
  - **Automatic mode (default)**: the halt clears itself automatically on the next reconciliation pass that confirms the mismatch is resolved. No human action required.
  - **Manual mode**: an automatic reconciliation that resolves the mismatch does **not** clear the halt. The engine continues reporting the instrument/account as halted (via a status method and continuous event-bus emission) until the integrator's own code explicitly calls a clear method (e.g., `engine.clear_halt(instrument)`).
  - Because the engine has no UI of its own, **manual mode is only practically usable if the integrator's own application observes and surfaces the halt event from the event bus** — this must be documented clearly as a requirement for anyone using manual mode.
- **Configurability (per adapter instance, all overridable, sensible-safe defaults)**:
  1. Whether auto-halt is enabled at all — **on by default**.
  2. Whether closing/reduce-only orders remain permitted during a halt — **on by default**.
  3. Whether clearing is automatic-on-clean-reconciliation or requires explicit manual acknowledgment — **automatic by default**.
- Every halt entry and clear event, regardless of configuration, is written to the audit trail and emitted on the event bus.

---

## 7. Risk Checks (v1)

A stateless, synchronous, ordered chain of validators run before every order is dispatched. Fail-fast: the first failing validator rejects the order immediately with a specific, typed error; later validators do not run.

**Execution order and specification:**

1. **Symbol/instrument validity** — confirms the instrument is known and tradable on the target adapter (i.e., its `InstrumentSpec` has been successfully fetched). Rejects with `InvalidSymbolError` if not. Runs first as the cheapest possible check.
2. **Order size / quantity bounds** — validates `quantity` against the instrument's `min_qty`/`max_qty`/`qty_precision` (from the cached `InstrumentSpec`), and separately against a **user-configured maximum order size/notional** (a safety ceiling independent of what the platform itself allows). Configurable per-instrument or globally.
3. **Price sanity bounds (fat-finger protection)** — applies to any order carrying a price (limit, stop, stop-limit) or where a reference price is available. Compares the order's price (and any stop-loss/take-profit trigger price attached to it) against an injected reference price, rejecting if deviation exceeds a configured percentage threshold. If no reference price is available, this validator **skips with a logged warning rather than blocking the order.**
4. **Duplicate / idempotent submission check** — confirms the `client_order_id` (generated by core before this chain runs) is not already submitted or in-flight, rejecting genuine accidental double-submission distinctly from a legitimate timeout-retry (handled via the status-check path in Section 9.2, not blind resubmission).
5. **Rate-limit self-throttling** — runs last, immediately before dispatch. Checks the engine's own tracked call budget for that platform/account (sourced from the adapter's reported limits, with an optional stricter user override) and proceeds, briefly queues, or rejects with `RateLimitError` — the engine must never fire a request purely to let the platform itself reject it.

**Additional hard requirements for this chain:**
- **TP/SL orders are not special-cased.** Any native TP/SL — whether attached at order placement or submitted as a separate follow-up order, for any TP/SL type the platform natively supports — passes through this identical chain, including the stop-price sanity check in step 3. No order-shaped request bypasses risk checks.
- **Configurability**: validator *thresholds* (size limits, price-deviation percentage, rate-limit budget) are user-configurable per instance. **Individual validators cannot be disabled in v1** — a deliberate safety stance given the eventual use with real capital.
- Every rejection from any validator is logged to the audit trail with the specific reason.

**Deferred to v2**: a full stateful risk engine — portfolio-level exposure limits, account-level drawdown/loss guards, real-time P&L feeding those guards, an automatic kill switch, correlation/concentration risk — built once the state mirror and reconciliation logic above are proven correct in production use. When built, it consumes the same state mirror and event bus already present in v1 (Section 13) — it does not change how state is tracked.

---

## 8. Instruments and Symbols

Designed against the most demanding platform (IBKR) so every platform fits without hacks — not designed from crypto and extended.

### 8.1 Canonical `Instrument` — structured object, not a string

Fields:
- `symbol` — base identifier (e.g. `EUR`, `BTC`, `AAPL`, `ES`)
- `quote_currency` — counter currency for pairs; `None` where not applicable (e.g. stocks)
- `asset_class` — classification tag: `SPOT`, `MARGIN_FX`, `CFD`, `FUTURES`, `OPTION`, `STOCK`, `BOND`, `FUND`. Used for behavior/routing decisions, not as the schema definition itself — adding a new asset class in the future must not require restructuring this object.
- `exchange` — required for IBKR; typically `None` for MT5/cTrader/crypto
- `currency` — contract/settlement currency (IBKR requires this distinct from quote currency)
- `expiry` — futures/options only
- `strike` / `option_right` (`CALL`/`PUT`) — options only
- `multiplier` — contract size, mainly futures/options
- `broker_symbol_override` — optional field populated by an MT5 adapter's alias table (8.3); never set by the user directly for other platforms

### 8.2 `InstrumentSpec` — trading rules per instrument
`tick_size`, `lot_size`, `min_qty`, `max_qty`, `min_notional`, `price_precision`, `qty_precision`. Fetched once per instrument via each platform's own metadata endpoint and cached, keyed by the canonical `Instrument` — this is the adapter's responsibility, distinct from live/streaming market data (Section 8.5).

### 8.3 Validation against real platform examples (required check before finalizing the object in code)
- **Bybit perpetual (BTC/USDT perp)**: `symbol=BTC, quote_currency=USDT, asset_class=FUTURES, expiry=None` (perpetual). Clean fit.
- **MT5 CFD (EURUSD on a broker using suffix `EURUSD.m`)**: `symbol=EUR, quote_currency=USD, asset_class=MARGIN_FX, broker_symbol_override="EURUSD.m"` supplied via the MT5 adapter's user-supplied alias table (broker-specific symbol strings are not standardized even within MT5 itself and cannot be hardcoded).
- **IBKR option (AAPL call, strike 200, exp Dec 2026)**: `symbol=AAPL, asset_class=OPTION, exchange=SMART, currency=USD, expiry=2026-12-18, strike=200, option_right=CALL, multiplier=100`. Clean fit.
- **cTrader forex (GBPUSD)**: `symbol=GBP, quote_currency=USD, asset_class=MARGIN_FX`. Clean fit.

This same validation exercise must be repeated for any new instrument type before it is assumed to fit the existing schema.

### 8.4 Display/shorthand string (convenience only, narrow scope)
- A shorthand string form (`BASE/QUOTE`, following the established convention used by widely-adopted unified crypto libraries, e.g. `BTC/USDT`) is available **only for genuinely two-sided instruments**: crypto spot/perp pairs and forex pairs on any platform (cTrader, MT5). For MT5 the shorthand always uses the canonical form (`EUR/USD`); the broker-specific literal string never appears in it.
- Instruments carrying expiry, strike, or multiplier (options, dated futures, CFDs with expiry) are **not** representable via this shorthand — they are constructed and displayed only via the full structured object.

### 8.5 Market data boundary
- The engine does not own a market-data pipeline. Any live reference price needed by the risk-check chain (Section 7, step 3) is supplied via a user-injected interface (e.g. a `get_reference_price(instrument)` callback) — the engine does not subscribe to or manage any market-data stream itself.
- Instrument metadata (8.2) is the one exception: it is near-static, platform-provided, and is the engine's responsibility to fetch and cache — it is not "market data" in the streaming sense.

---

## 9. Order Types, Idempotency, and Error Handling

### 9.1 Order types
- **Fixed, guaranteed core set** every adapter must implement identically: **Market, Limit, Stop, Stop-Limit**, each supporting standard time-in-force values (`GTC`, `IOC`, `FOK`, `DAY`). Code written against this set is portable across any adapter.
- Each adapter additionally **declares its own extra native capabilities** via a capability-reporting method (`supported_order_types()`). The core validates every order request against the target adapter's declared capabilities before dispatch; a request for an unsupported type raises `UnsupportedOrderTypeError` — never silently approximated or faked.
- **Synthetic/client-side order types** (trailing stop, chandelier exit, or anything requiring continuous engine-side price-watching not natively executed by the platform) are out of core scope entirely (Section 5). Native TP/SL attached at order placement is treated as an ordinary native order feature where the platform itself executes it, not as a synthetic type.

### 9.2 Idempotent submission
The core generates a unique client order ID before every dispatch attempt, included in every `place_order` call. If a submission call times out or its outcome is otherwise unknown, the core does not blindly retry — it first queries order status by that client order ID to determine whether it actually landed, and only submits fresh if it genuinely did not. Every adapter must support lookup by client order ID; if a future platform cannot support this, it is a hard limitation to flag explicitly, not work around silently.

### 9.3 Common exception hierarchy
Defined once in core; every adapter translates its platform's native errors into these before they reach core logic:
- `InsufficientBalanceError`
- `InvalidSymbolError`
- `RateLimitError`
- `OrderNotFoundError`
- `UnsupportedOrderTypeError`
- `DuplicateOrderIdError` — user-supplied `client_order_id` collides with an existing order (Section 9.2)
- `ConnectionError`
- `InstrumentHaltedError` / `AccountHaltedError` (Section 6.4)
- `PlatformError` — catch-all for anything that doesn't map cleanly, must carry the raw platform error as context so nothing is silently swallowed

Core logic (retry rules, risk decisions) is written once against these common types and works correctly for every current and future adapter.

### 9.4 Connection resilience
Reconnect, heartbeat, and backoff mechanics are platform-specific and live entirely inside each adapter, but every adapter reports connection status through the same lifecycle interface (`is_connected()`, plus a connection-state event on the event bus), so core and reconciliation logic react identically regardless of which platform's connection dropped. A reconnect must automatically trigger a reconciliation pass (Section 6.1).

### 9.5 Internal event bus
A first-class extension point present from v1, primarily consumed internally in v1 to feed the state mirror. This is the mechanism that allows v2 features (Section 13) to be built as pure additions — they subscribe to the same bus rather than requiring changes to core dispatch or adapter code.

---

## 10. Observability: Logging, Audit Trail, and History

### 10.1 Logging
- All logging is **structured (JSON) at the source** — every log event is a structured record (event type, timestamp, adapter-instance identifier, instrument, order id, correlation id, payload), never a free-text line. This is the single source of truth for all log data.
- **Human-readable output is a formatter on top of the same structured record**, not a separate logging system — one event, two renderings (e.g. via Python's standard `logging` with a JSON formatter, or `structlog`).
- **Operational logs** (debug/info/warning/error, for developers) are kept distinct from the **audit trail** (every order attempt, every risk-check accept/reject with its specific reason, every reconciliation result, every halt entry/clear). The audit trail is a durable, queryable business record and lives as rows in the state-mirror database (Section 6.2), not in rotated log files.

### 10.2 History accessors (v1) vs. computed statistics (deferred)
- Since every order, fill, position change, balance change, reconciliation event, and halt event is already captured in the state-mirror database for audit purposes, the engine must expose this data back to the integrator through simple, read-only, filterable accessor methods rather than requiring raw SQL against the internal schema. Required v1 accessors, each filterable by instrument and/or time range:
  - `get_order_history(...)`
  - `get_fill_history(...)`
  - `get_position_history(...)`
  - `get_balance_history(...)`
  - `get_reconciliation_events(...)`
  - `get_halt_events(...)`
- **Computed statistics/analytics are explicitly deferred, not built in v1** — realized/unrealized P&L, win rate, Sharpe-style metrics, drawdown curves, etc. require additional computation logic and methodology decisions (e.g. cost-basis method for P&L) that are a heavier, more opinionated concern than exposing raw history. This is deferred the same way the full risk engine is deferred, and can be built later (v2, or by the integrator directly) on top of the history accessors already available.
- No log-viewing or stats UI is built in v1; the explicit goal is that the structured logs, audit trail, and history accessors are already in a shape a future UI (v1 or v2) could consume without rework.

---

## 11. Testing Requirements

Given the eventual use with real capital, testing is a first-class deliverable, not an afterthought. Coverage must be comprehensive across all of the following tiers — this is a hard requirement, not a nice-to-have.

### 11.1 Unit tests — core logic in isolation
Using the engine's own public mock adapter (Section 14 — shipped as part of the core package, not a private test-only fixture), with no real network calls:
- Every risk-check validator (Section 7), tested individually with both passing and failing inputs, including edge cases (exact boundary values, missing reference price, zero/negative quantities).
- State mirror updates from simulated event-stream messages (fills, position/balance updates).
- **Every reconciliation mismatch case from the table in Section 6.3**, individually simulated, verifying the exact required handling (overwrite direction, halt scope, log content) for each.
- The halt state machine: entry, automatic clearing, manual clearing, and every configurability combination from Section 6.4.
- Idempotency logic: simulated timeout followed by retry, verifying no double submission occurs and the correct order state is reached.
- The common exception hierarchy: verifying core logic (e.g. retry-on-`RateLimitError`) behaves correctly against each exception type.
- History accessors: verifying filtered queries return correct results against seeded data.
- Sync facade correctness: verifying the persistent-event-loop pattern behaves correctly under concurrent sync calls from multiple threads, with no event-loop or connection churn.

### 11.2 Adapter tests — against each platform's real sandbox/testnet
Real network calls, no real funds at risk, for both Bybit and cTrader:
- Every guaranteed core order type (Market, Limit, Stop, Stop-Limit) and every declared adapter-specific extra type: place, modify, cancel, query status, verifying correct round-trip translation in both directions.
- Symbol/instrument metadata fetching and caching, verified against real instrument specs returned by the platform.
- Native error conditions deliberately triggered (e.g. invalid symbol, insufficient balance, oversized order) and verified to map to the correct unified exception.
- Connection resilience: forced disconnects, verifying reconnect, heartbeat, and the automatic reconciliation trigger all behave correctly.
- Websocket event stream correctness: verifying fills, position updates, and balance updates are correctly received and reflected in the state mirror.

### 11.3 Fixed go/no-go scenario checklist — required before any transition to live capital
This checklist is the explicit gate for live trading, not an informal judgment call. At minimum:
- An order is placed and correctly confirmed end-to-end.
- A timed-out order submission followed by the idempotency-driven retry path does not result in a double fill.
- A disconnect mid-session correctly triggers reconciliation on reconnect, and any injected mismatch during the disconnect is correctly detected and handled per Section 6.3.
- A deliberately oversized/invalid order is correctly rejected by the risk-check chain, with the correct validator identified as the cause.
- A halt is correctly entered on a simulated mismatch, correctly blocks new exposure-increasing orders while permitting closing orders, and correctly clears per the configured mode (automatic or manual).
- Full audit trail review: every action taken during the above scenarios is confirmed present and correctly detailed in the audit trail.

---

## 12. Documentation Site

A public documentation site is a required deliverable, not optional polish, given this is intended for other developers to adopt.

- **Tooling**: MkDocs Material, with `mkdocstrings` used to auto-generate API reference pages directly from source docstrings (avoiding hand-maintained duplication between code and docs).
- **Site title**: "Unified Trading Execution," with a plain, precise tagline (avoid hype language such as "blazing fast" or "next-gen" — a plain, confidence-inspiring tone suits infrastructure handling real capital better than marketing language).
- **Required structure:**
  - **Installation** — package installation instructions, including how v1 vs. v2 modules are installed separately (Section 13), and the unified import namespace (Section 2).
  - **Quickstart** — a minimal working example placing an order against a testnet, for both the async and sync APIs.
  - **Core concepts** — dedicated pages explaining: the `Instrument`/`UnifiedOrder`/`OrderResult` types, the adapter interface, the state mirror and reconciliation model, the risk-check chain, the halt mechanism, the event bus, and history accessors — written to be understandable without reading source code.
  - **Platform guides** — one page per adapter (Bybit, cTrader initially), covering credential setup, testnet/live switching, and any platform-specific notes (e.g. MT5's symbol-alias requirement, once that adapter exists).
  - **Examples** — worked examples: placing and tracking an order, subscribing to the event bus for fills, writing a custom price-feed injection, handling a halt in an integrator's own application, building a synthetic order type (e.g. trailing stop) on top of core primitives, querying history.
  - **API reference** — auto-generated from docstrings.
  - **FAQ / troubleshooting**.
  - **Contribution guide** — since v1 is open source, expectations for external contributions (issue process, PR process, coding standards) should be documented, referencing `CONTRIBUTING.md` (Section 14).

---

## 13. v1 / v2 Relationship and Extensibility Guarantee

This section exists to guarantee that **v1 is never rewritten to build v2 — v2 is strictly additive.**

- **Distribution structure**: one core package (v1) containing all unified types, dispatch/orchestration logic, the risk-check chain, the state mirror, the event bus, history accessors, and the Bybit/cTrader adapters. v2 capabilities (additional adapters such as MT5/IBKR, the full risk engine, the synthetic-order module, any future service/daemon layer) are distributed as **separate installable packages** that depend on the v1 core and register into the same adapter/plugin interface and the same event bus.
- **Why this holds without a rewrite** — every v2 feature identified consumes an interface that already exists in v1 for v1's own reasons, not something speculative:
  - New platform adapters (MT5, IBKR) implement the same adapter interface Bybit/cTrader already implement — core dispatch code does not change.
  - The full risk engine consumes the same state mirror and event bus v1's reconciliation logic already populates — it adds new logic, it does not change how state is tracked.
  - The synthetic-order module subscribes to the same event bus and price-feed hook already used internally in v1, and calls the same `place_order` path any other order uses.
  - A future service/daemon mode wraps the existing library interface behind a network API — the underlying engine object is unchanged.
- **Practical build requirement following from this**: every extension point v2 will need (adapter interface, event bus, `StateStore` interface, capability declarations) must be genuinely exercised and tested by v1 itself, not built speculatively and left unused.

---

## 14. Project Directory Structure

A **monorepo**, structured as multiple independently-packaged Python projects (a "packages" layout), managed via **uv workspaces**. This gives the packaging separation required by Section 13 (each package independently publishable to PyPI) while everything is developed and versioned together in one repo during solo development. Splitting into separate repos later, if ever needed, is a mechanical move — the package boundaries already exist.

```
unified-trading-execution/
├── packages/
│   ├── core/                                  # v1 core — zero platform-specific knowledge
│   │   ├── src/
│   │   │   └── unified_trading_execution/
│   │   │       ├── __init__.py
│   │   │       ├── types/                     # Instrument, UnifiedOrder, OrderResult, Position, Balance, InstrumentSpec
│   │   │       ├── adapter/                   # the Adapter ABC/Protocol itself — the contract, not implementations
│   │   │       ├── dispatch/                  # order dispatch orchestration, idempotency logic
│   │   │       ├── risk/                      # the risk-check validator chain
│   │   │       ├── state/
│   │   │       │   ├── store.py                # StateStore interface + SQLite default implementation
│   │   │       │   ├── reconciliation.py       # reconciliation logic, mismatch case handling
│   │   │       │   ├── halt.py                 # halt state machine
│   │   │       │   └── migrations/             # versioned schema migrations
│   │   │       ├── history/                   # read-only history/query accessors (Section 10.2)
│   │   │       ├── events/                     # internal event bus
│   │   │       ├── errors/                     # common exception hierarchy
│   │   │       ├── logging/                    # structured logging setup, JSON formatter, audit trail writer
│   │   │       ├── sync/                       # the sync facade (persistent event loop wrapper)
│   │   │       ├── testing/                    # PUBLIC mock adapter, shipped with the package (Section 11.1)
│   │   │       └── registry.py                 # plugin registration mechanism adapters hook into
│   │   ├── tests/
│   │   │   ├── unit/                           # mirrors src/ structure
│   │   │   └── conftest.py
│   │   └── pyproject.toml
│   │
│   ├── adapter-bybit/
│   │   ├── src/unified_trading_execution/bybit/    # translation logic only — REST/WS client, symbol mapping, error mapping
│   │   ├── tests/
│   │   │   ├── unit/                           # mocked HTTP/WS responses
│   │   │   └── integration/                    # real calls against Bybit testnet
│   │   └── pyproject.toml                      # depends on core package
│   │
│   ├── adapter-ctrader/
│   │   ├── src/unified_trading_execution/ctrader/
│   │   ├── tests/{unit,integration}/
│   │   └── pyproject.toml                      # depends on core package
│   │
│   └── (v2, later) adapter-mt5/, adapter-ibkr/, risk-engine/, synthetic-orders/
│       # same shape as above — this structurally proves the "additive, no core rewrite"
│       # guarantee from Section 13, not just a design promise
│
├── examples/                                   # runnable scripts referenced by the docs site
│   ├── quickstart_async.py
│   ├── quickstart_sync.py
│   ├── event_bus_fills.py
│   ├── custom_risk_validator.py
│   ├── handling_halts.py
│   └── history_queries.py
│
├── docs/                                       # MkDocs Material source
│   ├── mkdocs.yml
│   ├── index.md
│   ├── installation.md
│   ├── quickstart.md
│   ├── concepts/                               # Instrument, adapter interface, state mirror, risk checks, halting, event bus, history
│   ├── platforms/                              # bybit.md, ctrader.md
│   └── reference/                              # mkdocstrings auto-generated, minimal hand-written content
│
├── scripts/                                    # dev tooling, e.g. a runner for the Section 11.3 go/no-go scenario checklist
│
├── .github/
│   └── workflows/                              # CI: lint, type-check, unit tests per package (matrix across supported Python versions),
│                                                # integration tests against testnet (secrets-gated), docs build/deploy
│
├── .import-linter.toml                         # enforces the Section 4 layering rule: core must never import adapter code
├── .env.example                                # documents required env var names for integration tests; no real values
├── .pre-commit-config.yaml                     # ruff check, ruff format --check, mypy — run before commit
├── .gitignore
├── LICENSE                                     # Apache 2.0, root-level (governs v1 core + free adapters)
├── CONTRIBUTING.md
├── CHANGELOG.md
├── README.md
└── pyproject.toml                              # uv workspace root: declares packages/* as members, shared dev-dependencies
```

**Notes on specific structural decisions (these enforce rules from this document, not just organize files):**
- `packages/core/src/.../adapter/` contains only the interface definition, never a concrete implementation — this makes any accidental platform-specific logic leaking into core visible immediately in code review.
- `packages/core/src/.../testing/` is a **public** module (not buried in `tests/`) — both the project's own unit tests and any integrator's own strategy tests are expected to depend on it, since it's the officially supported way to test code built against this engine without hitting a real testnet.
- Every adapter package has its own `tests/integration/` (real testnet calls) separate from `tests/unit/` (mocked) — this maps directly onto Section 11.1/11.2 and lets CI run fast unit tests on every commit while integration tests run less frequently or gated behind secrets availability.
- `examples/` is separate from `docs/` but referenced by it — runnable, testable example scripts (can be executed in CI to catch documentation rot) distinct from the prose that explains them.
- A **`src/` layout** inside each package (rather than a flat package-at-repo-root layout) is standard, deliberate Python packaging practice — it prevents accidentally importing an uninstalled local package during testing and catches packaging mistakes early.

---

## 15. Development Tooling and Standards

- **Python version support**: minimum **Python 3.11**. Chosen specifically (not just "latest") because 3.11 introduces `asyncio.TaskGroup` and `asyncio.timeout()`, both directly useful for this engine's concurrent order-dispatch and network-timeout handling, and because 3.11 has meaningfully better asyncio performance than earlier versions. Declared via `requires-python = ">=3.11"` in each package's `pyproject.toml`, and verified (not just claimed) by running the full test suite across a Python version matrix (e.g. 3.11, 3.12, 3.13) in CI.
- **Workspace/dependency management**: **uv**, using its native workspace support (`packages/*` declared as workspace members in the root `pyproject.toml`). Chosen for fast dependency resolution/installs across multiple local packages during active development, and a single consistent lockfile across the workspace while each package still ships its own independently publishable `pyproject.toml`.
- **Linting and formatting**: **Ruff**, configured via `[tool.ruff]` in `pyproject.toml`, with `target-version` matching the Python 3.11 floor. Used live in-editor (via the Ruff editor extension) during coding, enforced via `.pre-commit-config.yaml` before every commit, and re-verified in CI (`ruff check`, `ruff format --check`) as the final gate.
- **Type checking**: **mypy**, run in strict mode, given correctness is an explicit, stated goal of this engine and it handles real trading logic. Run locally (pre-commit) and in CI.
- **Layering enforcement**: **import-linter**, configured to fail the build if core imports anything from an adapter package — turns the Section 4 layering rule into an enforced constraint rather than a convention that can be forgotten under deadline pressure.
- **Pre-commit hooks**: Ruff (lint + format check), mypy, and import-linter all run automatically on `git commit`, so problems surface before CI rather than after a push.

**Explicitly deferred, not covered in this document** (to be addressed once there is a working v1 to release): the release/publishing process (e.g. manual vs. automated PyPI publishing), formal semantic-versioning/API-stability policy for the core adapter interface, and a security disclosure policy. These are real future needs, not omissions — they are intentionally out of scope until there is something ready to publish.

---

## 16. Summary of Explicit Configuration Points

For quick reference, every place this document requires user-facing configurability:
- `StateStore` backend (SQLite default, swappable) and its storage location/path.
- Auto-halt enabled/disabled; closing-orders-permitted-during-halt; automatic vs. manual halt clearing (all per adapter instance).
- Risk-check validator thresholds: max order size/notional, price-deviation percentage for fat-finger protection, rate-limit budget.
- Testnet vs. live endpoint switch per adapter.
- MT5 broker-specific symbol alias table (once the MT5 adapter exists, v2).
- Credentials, supplied however the integrator prefers (env vars, secrets manager, config file, etc.) — the engine itself never dictates or parses a config format.

---

This document reflects everything discussed and agreed, including final naming and project structure. Nothing described here should be considered optional unless explicitly marked as deferred to v2 or explicitly marked as out of scope for this document.

---

## 17. Interface Contracts

This section defines every cross-cutting abstraction the current document names but does not specify. These are load-bearing contracts: v2 extensibility (Section 13), the adapter layering rule (Section 4), and the "single source of truth" principle all depend on having these interfaces locked down before implementation begins. Every type here follows the same principles as the rest of the document:

- **Single source of truth**: each concept (order, position, event) has exactly one definition, in core, shared by every adapter and every future v2 module — never duplicated or redefined per platform.
- **No business logic in data types**: these are pure data structures (frozen where possible, hashable where used as keys). Logic lives in the modules that consume them (dispatch, risk, reconciliation, history).
- **Highly optimised implementation**: the runtime code operating on these types must be written for performance from the start — use the state store's own query engine (SQL) for filtering and aggregation rather than pulling unfiltered result sets into Python; the risk-check chain must be a tight synchronous call with no allocations on the hot path beyond what the validators themselves require.
- **Correctness over convenience**: every type enforces its invariants at construction (no invalid combinations silently accepted); every adapter method must translate all platform-native errors into the common exception hierarchy before they cross the adapter boundary — an untranslated error is a bug; every interface method documents its preconditions and failure modes explicitly.

### 17.1 Shared enum types

All enums are `StrEnum` (or a frozen equivalent that carries both a machine-readable value and a human-readable label). Adding a new member to any enum must not be a breaking change for existing adapters — adapters must handle unknown values gracefully by mapping to `PlatformError` or declaring the feature unsupported, never by raising an unhandled enumeration error.

```
OrderType      = MARKET | LIMIT | STOP | STOP_LIMIT
OrderSide      = BUY | SELL
TimeInForce    = GTC | IOC | FOK | DAY
OrderStatus    = PENDING | OPEN | PARTIALLY_FILLED | FILLED | CANCELLED | REJECTED | EXPIRED
AssetClass     = SPOT | MARGIN_FX | CFD | FUTURES | OPTION | STOCK | BOND | FUND
OptionRight    = CALL | PUT
HaltState      = ACTIVE | HALTED
HaltClearMode  = AUTOMATIC | MANUAL
```

### 17.2 Instrument (restated formally from Section 8.1)

```python
@dataclass(frozen=True, slots=True)
class Instrument:
    symbol: str
    quote_currency: str | None
    asset_class: AssetClass
    exchange: str | None
    currency: str | None
    expiry: date | None
    strike: Decimal | None
    option_right: OptionRight | None
    multiplier: int | None
    broker_symbol_override: str | None
```

Frozen, hashable, slots-based — zero per-instance dict overhead, usable as a dict key and cache lookup key. Equality and hashing consider all fields. The shorthand `str()` form (`BASE/QUOTE`) is available only for crypto spot/perp pairs and forex pairs on any platform; all other instruments raise `ValueError` on `str()` and must be displayed via explicit field access.

**Invariants enforced at construction:**
- `symbol` must be non-empty and uppercase.
- `expiry` is required iff `asset_class == OPTION`. For `FUTURES`, `expiry=None` means a perpetual contract (e.g., Bybit BTC/USDT perpetual) — dated futures carry an explicit expiry; perpetuals do not. This is validated by the Section 8.3 examples.
- `strike` and `option_right` are required iff `asset_class == OPTION`.
- `multiplier` is required for `FUTURES` and `OPTION`, optional otherwise.
- `broker_symbol_override` must never be set by user code — it is populated exclusively by the MT5 adapter's alias table (v2). Rather than attempting to enforce this at runtime via fragile caller-identity checks, `broker_symbol_override` is not exposed on the public `Instrument(...)` constructor at all. The adapter accesses it through a standalone module-level function `_with_broker_override(instrument, override)` in `unified_trading_execution.types.instrument`. It is underscore-convention private — imported by adapter packages but never present in user-facing examples, docs, or IDE autocomplete on the `Instrument` class. Core never reads or branches on this field — it is purely a passthrough for the adapter's own symbol translation.

### 17.3 InstrumentSpec (restated from Section 8.2)

```python
@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    tick_size: Decimal
    lot_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    price_precision: int
    qty_precision: int
```

Frozen, fetched once per instrument via each platform's metadata endpoint, cached indefinitely keyed by `Instrument`. The adapter may attach platform-specific extension fields for its own internal use; core never accesses beyond these seven fields.

**Cache invalidation:** the cache lives for the lifetime of the adapter instance. If a platform changes instrument specs mid-session (exchange halts a contract, adjusts tick size), the adapter must detect this via its websocket event stream and invalidate the cached entry, forcing a re-fetch on the next access. This is an adapter-internal concern — core is unaware of it.

### 17.4 Quantity, price, and precision conventions

All quantities and prices flow through the engine as `Decimal` — never `float`. Floating-point arithmetic is unacceptable in a system handling financial quantities where rounding errors compound into real monetary discrepancies.

**Quantity convention:** `quantity` is always denominated in units of the **base asset**:
- Crypto pairs: the base currency (e.g., BTC in BTC/USDT).
- Forex pairs: units of the base currency (e.g., EUR in EUR/USD) — never lots.
- Stocks: number of shares.
- Futures/options: number of contracts.
- CFDs: number of contracts.

The adapter is responsible for translating between this canonical representation and the platform's native quantity convention. Core logic never branches on asset class for quantity handling — the convention is uniform.

**Price convention:** prices are always denominated in the quote currency (for pairs) or the settlement currency (for stocks/options/futures). `price_precision` and `qty_precision` from `InstrumentSpec` define the maximum decimal places the platform accepts; core rounds to these precisions immediately before dispatch, after all risk checks have run at full precision.

### 17.5 UnifiedOrder

```python
@dataclass(slots=True)
class UnifiedOrder:
    instrument: Instrument
    order_type: OrderType
    side: OrderSide
    quantity: Decimal
    time_in_force: TimeInForce

    # Idempotency: generated by core (UUID7, time-ordered, DB-friendly) if the
    # user does not supply one. If user-supplied, core validates uniqueness
    # against ALL orders ever in the state store — collision with any existing
    # order (active or terminal) raises DuplicateOrderIdError. Users who need
    # external correlation without permanent uniqueness constraints should use
    # client_tag instead. The default UUID7 is zero-config safe.
    client_order_id: str | None = None

    price: Decimal | None = None          # LIMIT, STOP_LIMIT
    stop_price: Decimal | None = None     # STOP, STOP_LIMIT
    reduce_only: bool = False
    client_tag: str | None = None         # user's own reference; never read by engine

    # TP/SL attached at placement. Only valid when the adapter declares
    # TP/SL support via capabilities. Processed through the same risk-check
    # chain as the parent order (Section 7, hard requirement #1).
    take_profit: TpSlAttachment | None = None
    stop_loss: TpSlAttachment | None = None

@dataclass(frozen=True, slots=True)
class TpSlAttachment:
    trigger_price: Decimal
    limit_price: Decimal | None = None    # None = market execution when triggered
```

**Invariants enforced at construction:**
- `price` is required iff `order_type in (LIMIT, STOP_LIMIT)`.
- `stop_price` is required iff `order_type in (STOP, STOP_LIMIT)`.
- `quantity > 0` — side determines direction; quantity is always a positive magnitude.
- `price > 0` when present; `stop_price > 0` when present.
- `take_profit` and `stop_loss` must not both reference the same `client_order_id` as an existing TP/SL attachment on another order.

These invariants are enforced once in the `UnifiedOrder` constructor — adapters never re-validate them, and core dispatch does not need to. A `UnifiedOrder` that exists in memory is, by construction, structurally valid.

### 17.6 OrderModification

```python
@dataclass(slots=True)
class OrderModification:
    client_order_id: str          # the order to modify
    price: Decimal | None = None
    stop_price: Decimal | None = None
    quantity: Decimal | None = None
    take_profit: TpSlAttachment | None = None
    stop_loss: TpSlAttachment | None = None
```

At least one optional field must be set. The adapter applies only the fields the target platform supports modifying; an unsupported modification field raises `UnsupportedOrderTypeError` with a message naming the unsupported field. Core runs the full risk-check chain against the resulting order (applying modifications on top of the current state from the state store) before dispatching — a modification that produces an order that would fail a risk check is rejected before it reaches the platform.

### 17.7 OrderResult

```python
@dataclass(frozen=True, slots=True)
class OrderResult:
    client_order_id: str
    platform_order_id: str | None      # None only if rejected before platform assigned an ID
    status: OrderStatus
    filled_quantity: Decimal
    average_fill_price: Decimal | None # None until first fill
    created_at: datetime               # UTC, timezone-aware
    updated_at: datetime               # UTC, timezone-aware
```

Returned by `place_order`, `modify_order`, `cancel_order`, and `get_order_by_client_id`. All timestamps are UTC with explicit `tzinfo` — naive datetimes are rejected at construction.

### 17.8 OrderRecord (persistent)

The full auditable lifecycle of one order, stored as a single row in the state store. Combines all fields from `UnifiedOrder` and `OrderResult` plus the `correlation_id` (Section 17.14). The history accessors (Section 10.2) return `OrderRecord` instances.

```python
@dataclass(frozen=True, slots=True)
class OrderRecord:
    # From UnifiedOrder
    instrument: Instrument
    order_type: OrderType
    side: OrderSide
    quantity: Decimal
    time_in_force: TimeInForce
    client_order_id: str
    price: Decimal | None
    stop_price: Decimal | None
    reduce_only: bool
    client_tag: str | None
    take_profit: TpSlAttachment | None
    stop_loss: TpSlAttachment | None

    # From OrderResult
    platform_order_id: str | None
    status: OrderStatus
    filled_quantity: Decimal
    average_fill_price: Decimal | None

    # Lifecycle
    correlation_id: str
    created_at: datetime
    updated_at: datetime
```

### 17.9 Position and Balance

```python
@dataclass(frozen=True, slots=True)
class Position:
    instrument: Instrument
    quantity: Decimal             # positive = long, negative = short
    average_entry_price: Decimal
    updated_at: datetime          # UTC, timezone-aware

@dataclass(frozen=True, slots=True)
class Balance:
    currency: str
    free: Decimal                 # available for new orders
    used: Decimal                 # locked in open orders / margin
    total: Decimal                # free + used
    updated_at: datetime          # UTC, timezone-aware
```

The `total == free + used` invariant is enforced at construction.

### 17.10 Adapter ABC — complete method surface

This is the complete contract every platform adapter must implement. No adapter method contains business logic, retry policy, or risk decisions — those live in core and consume the values these methods return. Every method that can fail translates platform-native errors into the common exception hierarchy (Section 9.3) before the error crosses the adapter boundary.

```python
class Adapter(ABC):
    # ---- Identification ----
    @property
    def platform_name(self) -> str: ...
    @property
    def account_id(self) -> str: ...

    # ---- Connection lifecycle ----
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    def is_connected(self) -> bool: ...

    # ---- Order operations ----
    async def place_order(self, order: UnifiedOrder) -> OrderResult: ...
    async def modify_order(self, modification: OrderModification) -> OrderResult: ...
    async def cancel_order(self, client_order_id: str) -> OrderResult: ...
    async def get_order_by_client_id(self, client_order_id: str) -> OrderResult | None: ...

    # ---- Instrument metadata ----
    async def fetch_instrument_spec(self, instrument: Instrument) -> InstrumentSpec: ...

    # ---- Capability reporting ----
    def supported_order_types(self) -> frozenset[OrderType]: ...

    # ---- Rate limits ----
    async def get_rate_limits(self) -> RateLimits: ...

@dataclass(frozen=True, slots=True)
class RateLimits:
    requests_per_interval: int
    interval_seconds: float
    remaining: int
    reset_at: datetime
```

**Additional requirements on every adapter:**

- **Constructor**: accepts its own configuration (credentials, testnet/live switch, any platform-specific options) and a reference to the `EventBus`. The adapter publishes translated events to this bus from its internal websocket handlers. The adapter never holds a reference to the `StateStore` — it produces events; core's state mirror consumes them.
- **Reconnect, heartbeat, and backoff** are adapter-internal implementation details. The adapter must publish a `ConnectionStateEvent` on the event bus on every state change (connected → disconnected and vice versa). Core's reconciliation logic reacts to these events — it does not need to know the platform-specific mechanics behind them.
- **`place_order`**: receives a fully validated `UnifiedOrder` (risk checks already passed). Translates it to the platform's native request format, dispatches it, and returns an `OrderResult`. If the platform provides a native TP/SL attachment mechanism, the adapter uses it for any attached `take_profit`/`stop_loss`; if the platform does not support native TP/SL, the adapter raises `UnsupportedOrderTypeError` — it never simulates or approximates.
- **`supported_order_types`**: must always return at minimum `{MARKET, LIMIT, STOP, STOP_LIMIT}` (the guaranteed portable set). Core dispatch validates every order request against this set before calling `place_order`.
- **`get_rate_limits`**: returns the platform's current rate-limit state. Queried by the self-throttling validator (Section 7, step 5). Core may cache this result briefly (TTL determined by `interval_seconds`) rather than calling it before every dispatch.
- **Error translation**: every public method must wrap platform-native errors in the appropriate exception from the common hierarchy before propagating. A platform-native exception escaping the adapter is a bug — core must never need to know about Bybit's or cTrader's internal error codes.

### 17.11 StateStore interface

All state mirror read/write operations go through this interface. The default v1 implementation is SQLite via `aiosqlite`. The interface is designed so that a future Postgres or Redis backend (v2, multi-instance) can be swapped in with zero changes to core logic.

```python
class StateStore(ABC):
    # ---- Single-record write (called by state mirror event handlers) ----
    async def upsert_position(self, position: Position) -> None: ...
    async def upsert_balance(self, balance: Balance) -> None: ...
    async def upsert_order(self, order: OrderRecord) -> None: ...
    async def upsert_fill(self, fill: FillRecord) -> None: ...

    # ---- Audit trail write (append-only) ----
    async def write_audit_event(self, event: AuditEvent) -> None: ...
    async def write_reconciliation_event(self, event: ReconciliationEvent) -> None: ...
    async def write_halt_event(self, event: HaltEvent) -> None: ...

    # ---- Single-record read (for runtime dispatch/reconciliation logic) ----
    async def get_position(self, instrument: Instrument) -> Position | None: ...
    async def get_balance(self, currency: str) -> Balance | None: ...
    async def get_order(self, client_order_id: str) -> OrderRecord | None: ...

    # ---- Filtered queries (for history accessors, Section 10.2) ----
    # Replaces the earlier get_all_positions / get_all_balances with
    # filterable query_* methods that support instrument, time-range,
    # and limit parameters — strictly more useful and aligned with the
    # history-accessor API surface.
    async def query_orders(self, *,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[OrderRecord]: ...

    async def query_fills(self, *,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[FillRecord]: ...

    async def query_positions(self, *,
        instrument: Instrument | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[Position]: ...

    async def query_balances(self, *,
        currency: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[Balance]: ...

    async def query_reconciliation_events(self, *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[ReconciliationEvent]: ...

    async def query_halt_events(self, *,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[HaltEvent]: ...

    # ---- Lifecycle ----
    async def initialize(self) -> None: ...  # create tables, run pending migrations
    async def flush(self) -> None: ...       # ensure pending writes are durable (no-op by default)
    async def close(self) -> None: ...       # close connection
```

**Performance requirement for the SQLite implementation:**
- `query_*` methods must execute as single SQL queries with appropriate indexes — no in-Python filtering of unfiltered result sets.
- Bulk reconciliation writes must use batched inserts (`executemany` / a single transaction), not individual writes per record.
- The SQLite connection must be configured with `WAL` journal mode and `synchronous=NORMAL` for write performance without sacrificing durability.

**`AuditEvent`** is a single flat dataclass for order-lifecycle audit records (placed, modified, cancelled). It carries: `event_id` (UUID7), `timestamp` (UTC, timezone-aware), `adapter_name`, `account_id`, `correlation_id`, `event_type` (string: `"order.placed"` / `"order.modified"` / `"order.cancelled"`), and a `payload` dict for structured per-event metadata. Reconciliation and halt events have their own dedicated audit types (`ReconciliationEvent`, `HaltEvent`) and their own `write_reconciliation_event` / `write_halt_event` store methods — they do not go through `write_audit_event`.

**`FillRecord`:**
```python
@dataclass(frozen=True, slots=True)
class FillRecord:
    client_order_id: str
    platform_fill_id: str
    instrument: Instrument
    fill_quantity: Decimal
    fill_price: Decimal
    fill_timestamp: datetime
    fee_currency: str | None
    fee_amount: Decimal | None
    correlation_id: str
```

### 17.12 Event bus and event types

The event bus is a simple synchronous pub/sub mechanism within the async event loop — no queueing, no persistence, no cross-process delivery in v1. Subscribers register with `subscribe(event_type: type[E], callback: Callable[[E], None])`. Publishers call `publish(event: Event)` which invokes all matching subscribers synchronously in registration order. Subscribers must not raise — an exception in a subscriber is logged and does not prevent remaining subscribers from running. Subscriber callbacks are synchronous (`Callable[[E], None]`, not `Awaitable`); this keeps the hot path allocation-free and avoids async fan-out latency.

All events inherit from a common base. Every event emitted on the bus is also written to the audit trail — not by a subscriber (which would require async, breaking the synchronous callback contract), but by the Engine itself: the Engine calls `state_store.write_audit_event()` immediately after `event_bus.publish()` in the same coroutine. This guarantees no event can be "on the bus but not in the audit trail" without introducing async subscribers or an internal queue.

```python
@dataclass(frozen=True, slots=True)
class Event:
    event_id: str                  # UUID7
    timestamp: datetime            # UTC, timezone-aware
    adapter_name: str
    account_id: str
    correlation_id: str | None     # None for events not tied to a user action (e.g., spontaneous balance update)

@dataclass(frozen=True, slots=True)
class FillEvent(Event):
    fill: FillRecord

@dataclass(frozen=True, slots=True)
class PositionUpdateEvent(Event):
    position: Position             # new state after update

@dataclass(frozen=True, slots=True)
class BalanceUpdateEvent(Event):
    balance: Balance               # new state after update

@dataclass(frozen=True, slots=True)
class ConnectionStateEvent(Event):
    connected: bool

@dataclass(frozen=True, slots=True)
class OrderPlacedEvent(Event):
    order: OrderRecord

@dataclass(frozen=True, slots=True)
class OrderModifiedEvent(Event):
    order: OrderRecord             # new state
    previous: OrderRecord          # state before modification

@dataclass(frozen=True, slots=True)
class OrderCancelledEvent(Event):
    client_order_id: str
    instrument: Instrument

@dataclass(frozen=True, slots=True)
class ReconciliationCompleteEvent(Event):
    mismatches: tuple[ReconciliationMismatch, ...]
    # empty tuple = clean reconciliation

@dataclass(frozen=True, slots=True)
class HaltEnteredEvent(Event):
    scope: Literal["instrument", "account"]
    instrument: Instrument | None  # None when scope="account"
    reason: str                    # machine-readable: "position_quantity_mismatch", "balance_mismatch"
    detail: str                    # human-readable description of the discrepancy

@dataclass(frozen=True, slots=True)
class HaltClearedEvent(Event):
    scope: Literal["instrument", "account"]
    instrument: Instrument | None
    cleared_by: Literal["automatic", "manual"]

@dataclass(frozen=True, slots=True)
class ReconciliationMismatch:
    mismatch_type: Literal[
        "position_quantity", "balance", "orphan_on_platform",
        "orphan_in_local", "partial_fill"
    ]
    instrument: Instrument | None
    local_value: str               # JSON-serialized for audit portability
    platform_value: str
```

### 17.13 Callback signatures

```python
# Section 8.5 — injected reference price for fat-finger validation (Section 7, step 3).
# Must return immediately (no I/O). Returns None when no price is available,
# in which case the validator skips with a logged warning.
GetReferencePrice = Callable[[Instrument], Decimal | None]
```

The callback is called synchronously within the risk-check chain. If the integrator's price source is async (e.g., a websocket feed), they must maintain their own thread-safe cache and have the callback read from it — this keeps the risk-check chain a pure synchronous function with no await points.

### 17.14 Cross-cutting implementation decisions

**Correlation IDs:** A single `correlation_id` (UUID7, time-ordered, DB-friendly) is generated by core at the start of each top-level user action (`place_order`, `modify_order`, `cancel_order`). It is attached to every log line, audit event, and bus event produced by that action's entire lifecycle — including downstream effects like fills, position updates, and reconciliation events triggered by that order. This allows tracing an order from the initial user call through every fill and state change it produces, without the integrator building their own distributed tracing plumbing. The correlation ID is available to the user on the returned `OrderResult` (via `OrderRecord`) but is never set by the user — it is an engine-internal tracing primitive.

**Client order ID generation:** Core generates a UUID7 `client_order_id` for every `place_order` call where the user does not supply one. UUID7 is chosen over UUID4 specifically because its time-ordered prefix makes it index-friendly in both SQLite B-trees and any future Postgres backend — monotonically increasing keys avoid page splits. **Note:** `uuid.uuid7()` is not available in Python's stdlib before 3.14; the `uuid7` package (a single-file, zero-dependency backport) is declared as a core dependency in `pyproject.toml` to bridge the Python 3.11–3.13 gap. If the user supplies their own `client_order_id`, core validates it is unique across **all orders ever** in the state store before accepting it — not just currently-active ones. Duplicate `client_order_id` raises `DuplicateOrderIdError`. The `client_order_id` is the permanent primary key for order identity everywhere — in the state store, in the audit trail, on the event bus, and in the idempotency path (Section 9.2). Users who need external correlation without permanent uniqueness constraints should use `client_tag` instead of supplying their own `client_order_id`.

**Graceful shutdown:** `engine.shutdown()` (sync) and `engine.ashutdown()` (async) perform an ordered teardown:
1. Flush all pending audit-trail writes to the state store.
2. Disconnect all adapters (cancel in-flight requests, close websockets).
3. Close the state store connection.
4. Stop the background event loop (sync facade only).

After shutdown, the engine instance is permanently unusable — calling any method on a shut-down engine raises `EngineShutdownError`. The shutdown method is idempotent: calling it on an already-shut-down engine is a no-op, not an error.

**Thread safety:** The async core runs on a single event loop. The sync facade serializes calls from external threads onto that loop via `asyncio.run_coroutine_threadsafe`. No internal lock is exposed to user code. Concurrent sync calls from multiple threads are safe — they are linearized by the event loop. Concurrent async calls are safe by definition (single-threaded asyncio). The `StateStore` implementation (SQLite) must be used from the event loop thread only — all access is naturally serialized.

**Multi-account:** One engine instance manages exactly one adapter instance (one platform, one account). Running multiple accounts or multiple platforms requires the integrator to create multiple engine instances. This keeps the state-mirror storage file naturally scoped to a single account, avoids cross-account contamination in the event bus, and eliminates an entire class of multi-tenancy bugs (wrong-account order routing).

**Data integrity guarantees** (in addition to the reconciliation mechanism in Section 6):
- Every write to the state store that is part of a logical group (e.g., an order fill that also updates the position) occurs within a single transaction — partial writes are impossible.
- The audit trail is append-only. Once written, an audit event is never mutated or deleted. The `write_audit_event` method must reject any call that attempts to overwrite or modify an existing event.
- Timestamps are always generated by core (using `datetime.now(tz=timezone.utc)`), never by adapter code — the engine is the single source of truth for event ordering, not the platform.

**Performance guardrails:**
- The risk-check chain is synchronous and must complete in under 1ms on commodity hardware — the network round-trip is the dominant cost, and the risk checks must not add meaningful overhead to it.
- History accessor queries over large date ranges must use indexed columns and return in constant-ish time regardless of total row count — full table scans in the hot reconciliation path are unacceptable.
- The event bus dispatch (publish → all subscribers notified) must be O(subscribers) with no allocations beyond the event object itself — no intermediate queues, no async fan-out within a single publish call.
- The reconciliation diff and history query result processing must use the state store's own query engine (SQL) for filtering and aggregation — do NOT pull unfiltered result sets into Python and process them in per-row loops. The SQLite implementation with proper indexes and batched inserts is sufficient; no external dataframe/vectorisation library is needed or approved.
