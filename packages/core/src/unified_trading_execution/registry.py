"""Plugin registration mechanism — adapters register themselves here.

Each adapter package calls register_adapter() at import time so core
dispatch can discover available adapters without importing platform code.
"""

from __future__ import annotations
