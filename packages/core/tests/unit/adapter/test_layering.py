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
PACKAGE_ROOT = CORE_SRC / "unified_trading_execution"
COREMOD = "unified_trading_execution"
ADAPTER_NAMES = ("binance", "bybit", "ctrader", "hyperliquid", "ibkr", "mt5")


def _find_py_files(root: Path) -> list[Path]:
    py_files: list[Path] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for fname in filenames:
            if fname.endswith(".py"):
                py_files.append(Path(dirpath) / fname)
    return py_files


def _is_adapter_file(py_file: Path) -> bool:
    rel = py_file.relative_to(PACKAGE_ROOT)
    return bool(rel.parts) and rel.parts[0] in ADAPTER_NAMES


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
        for py_file in _find_py_files(PACKAGE_ROOT):
            rel = py_file.relative_to(PACKAGE_ROOT)
            # Adapter trees live under the package for packaging, not core logic.
            if "tests" in rel.parts or _is_adapter_file(py_file):
                continue
            all_imports |= _extract_imports(py_file)
        return all_imports

    @pytest.mark.parametrize("adapter_name", ADAPTER_NAMES)
    def test_core_does_not_import_adapter(self, core_imports, adapter_name):
        prefix = f"{COREMOD}.{adapter_name}"
        violations = {i for i in core_imports if i == prefix or i.startswith(f"{prefix}.")}
        assert violations == set(), f"Core imports {adapter_name}: {violations}"


# ============================================================
# Contract 2 + 3: Adapter cross-import and independence checks
# ============================================================

# Adapter source directories — only check those that exist on disk
ADAPTER_DIRS: dict[str, Path] = {}
for adapter_name in ADAPTER_NAMES:
    candidate = PACKAGE_ROOT / adapter_name
    if candidate.is_dir():
        ADAPTER_DIRS[adapter_name] = candidate


def test_all_adapter_source_dirs_are_discovered():
    # An empty parametrize list silently skips the adapter contracts below.
    assert sorted(ADAPTER_DIRS) == sorted(ADAPTER_NAMES)


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
