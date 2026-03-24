"""EWZ to BOVA11 relative-value bridge."""

from __future__ import annotations

from global_arbitrage.strategies.bridge import HedgeRatioBridgeStrategy


class EwzBovaBridgeStrategy(HedgeRatioBridgeStrategy):
    """Translate EWZ into BRL and compare against BOVA11 or a local beta proxy."""
