"""Risk-check validator chain stub — Section 7 of the requirements.

Stateless, synchronous, ordered chain run before every order dispatch.
Fail-fast: the first failing validator rejects the order immediately.
"""

from __future__ import annotations
