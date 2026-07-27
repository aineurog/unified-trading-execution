# Changelog

All notable changes to the Unified Trading Execution project will be documented in this file.

## [0.1.0] — Unreleased

### Added
- Initial project scaffold: monorepo structure, core package, adapter skeletons.
- Core type definitions (`Instrument`, `UnifiedOrder`, `OrderResult`, etc.).
- `Adapter` ABC and `StateStore` ABC.
- Event bus interface and event type definitions.
- Common exception hierarchy.
- uv workspace configuration, linting (ruff), type checking (mypy), and layering enforcement (import-linter).
