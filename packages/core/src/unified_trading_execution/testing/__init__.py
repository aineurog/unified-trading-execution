"""Public mock adapter — shipped with the core package, not a private test-only fixture.

Both the project's own unit tests and any integrator's own strategy tests are
expected to depend on this module. It is the officially supported way to test
code built against this engine without hitting a real testnet.
"""

from __future__ import annotations
