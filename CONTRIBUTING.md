# Contributing to Unified Trading Execution

## Getting Started

1. Clone the repository.
2. Install uv (`curl -LsSf https://astral.sh/uv/install.sh | sh`).
3. Run `uv sync` in the repository root to install all workspace packages and dev dependencies.
4. Install pre-commit hooks: `uv run pre-commit install`.

## Project Structure

This is a uv workspace monorepo. See the [requirements document](unified-trading-execution-requirements.md) for the full architecture.

- `packages/core/` — Platform-agnostic types, dispatch, risk checks, state mirror, event bus.
- `packages/adapter-bybit/` — Bybit platform adapter.
- `packages/adapter-ctrader/` — cTrader platform adapter.

## Development Workflow

1. Create a branch for your work.
2. Write code, tests pass (`uv run pytest`).
3. Run linting: `uv run ruff check && uv run ruff format --check`.
4. Run type checking: `uv run mypy`.
5. Run layer enforcement: `uv run lint-imports`.
6. Commit — pre-commit hooks will run automatically.

## Code Standards

- **Adapter as translator, not decision-maker**: platform-specific code does not contain business logic, retry policy, or risk decisions.
- **Single source of truth**: any given piece of logic exists exactly once, in core.
- **Fail loud**: never silently swallow errors or approximate unsupported features.
- All public types are frozen `dataclass` with `slots=True` where possible.
- All quantities and prices are `Decimal`, never `float`.
- All timestamps are UTC with explicit `tzinfo`.
- Core must never import from an adapter package (enforced by import-linter).

## Testing

- Unit tests run against the public mock adapter — no real network calls.
- Integration tests (in each adapter's `tests/<adapter>_integration/`) require sandbox/testnet credentials.
- See `.env.example` for the required environment variables.

## Questions

Read the [requirements document](unified-trading-execution-requirements.md) first — it is the authoritative specification for every design decision in this project.
