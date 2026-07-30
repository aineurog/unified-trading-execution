"""Layering enforcement — Section 4 + Section 15 import-linter contracts.

These tests mechanically enforce the three layering rules via AST inspection
rather than via grimp/import-linter CLI (which has known limitations with
pkgutil-style namespace packages). The import-linter config in pyproject.toml
remains as the canonical contract definition; these tests are the runtime
enforcement.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path

import pytest

CORE_SRC = Path(__file__).resolve().parent.parent.parent.parent / "src"
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
COREMOD = "unified_trading_execution"

FORBIDDEN_ADAPTERS = [
    f"{COREMOD}.bybit",
    f"{COREMOD}.ctrader",
    f"{COREMOD}.mt5",
    f"{COREMOD}.ibkr",
]


def _find_py_files(root: Path) -> list[Path]:
    py_files: list[Path] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for fname in filenames:
            if fname.endswith(".py"):
                py_files.append(Path(dirpath) / fname)
    return py_files


def _extract_imports(filepath: Path) -> set[str]:
    """Return the set of module-level import targets in a Python file."""
    tree = ast.parse(filepath.read_text())
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module is not None:
                # For relative imports (level > 0), build absolute module path
                if node.level > 0:
                    # Resolve relative to file location
                    rel_to = filepath
                    for _ in range(node.level - 1):
                        rel_to = rel_to.parent
                    pkg = (
                        ".".join(rel_to.resolve().relative_to(CORE_SRC.resolve()).parts)
                        .removesuffix(".py")
                        .removesuffix(".__init__")
                    )
                    if node.module:
                        imports.add(f"{pkg}.{node.module}")
                    else:
                        imports.add(pkg)
                else:
                    imports.add(node.module)
    return imports


# ============================================================
# Contract 1: Core must never import adapter code
# ============================================================


class TestCoreNeverImportsAdapters:
    """Forbidden contract: unified_trading_execution -> adapter packages."""

    @pytest.fixture
    def core_imports(self):
        all_imports: set[str] = set()
        for py_file in _find_py_files(CORE_SRC):
            rel = py_file.relative_to(CORE_SRC)
            # Skip test files, they're not core
            if "tests" in rel.parts:
                continue
            all_imports |= _extract_imports(py_file)
        return all_imports

    def test_no_bybit_imports(self, core_imports):
        bybit_imports = {i for i in core_imports if "bybit" in i.lower()}
        assert bybit_imports == set(), f"Core imports bybit: {bybit_imports}"

    def test_no_ctrader_imports(self, core_imports):
        ctrader_imports = {i for i in core_imports if "ctrader" in i.lower()}
        assert ctrader_imports == set(), f"Core imports ctrader: {ctrader_imports}"

    def test_no_mt5_imports(self, core_imports):
        mt5_imports = {i for i in core_imports if "mt5" in i.lower()}
        assert mt5_imports == set(), f"Core imports mt5: {mt5_imports}"

    def test_no_ibkr_imports(self, core_imports):
        ibkr_imports = {i for i in core_imports if "ibkr" in i.lower()}
        assert ibkr_imports == set(), f"Core imports ibkr: {ibkr_imports}"


# ============================================================
# Contract 2 + 3: Adapter cross-import and independence checks
# ============================================================

# Adapter source directories — only check those that exist on disk
ADAPTER_DIRS: dict[str, Path] = {}
for adapter_name in ("bybit", "ctrader", "mt5", "ibkr"):
    candidate = (
        WORKSPACE_ROOT / "packages" / f"adapter-{adapter_name}" / "src" / COREMOD / adapter_name
    )
    if candidate.is_dir():
        ADAPTER_DIRS[adapter_name] = candidate


def _adapter_imports(adapter_name: str) -> set[str]:
    root = ADAPTER_DIRS.get(adapter_name)
    if root is None:
        return set()
    imports: set[str] = set()
    for py_file in _find_py_files(root):
        imports |= _extract_imports(py_file)
    return imports


class TestAdaptersNeverImportEachOther:
    """Forbidden contract: adapters must not import other adapters."""

    @pytest.mark.parametrize("adapter_name", sorted(ADAPTER_DIRS.keys()))
    def test_adapter_does_not_import_other_adapters(self, adapter_name):
        imports = _adapter_imports(adapter_name)
        others = set(ADAPTER_DIRS.keys()) - {adapter_name}
        violations = {
            i
            for i in imports
            for other in others
            if f"{COREMOD}.{other}" in i or i.startswith(f"{other}.")
        }
        assert violations == set(), f"{adapter_name} imports other adapters: {violations}"


class TestAdaptersOnlyImportCore:
    """Independence contract: adapters only import from core, stdlib, and third-party."""

    @pytest.mark.parametrize("adapter_name", sorted(ADAPTER_DIRS.keys()))
    def test_adapter_only_imports_from_core(self, adapter_name):
        imports = _adapter_imports(adapter_name)
        # Allow: unified_trading_execution.* (core), stdlib, third-party
        # Forbid: any other unified_trading_execution sub-package not in allowed set
        other_adapters = set(ADAPTER_DIRS.keys()) - {adapter_name}
        violations = set()
        for imp in imports:
            if not imp.startswith(COREMOD):
                continue  # stdlib or third-party — fine
            if imp == COREMOD:
                continue  # top-level core import — fine
            # It's a unified_trading_execution.* import
            submodule = imp[len(COREMOD) + 1 :]
            top_level = submodule.split(".")[0]
            if top_level in other_adapters:
                violations.add(imp)
        assert violations == set(), f"{adapter_name} imports forbidden modules: {violations}"
