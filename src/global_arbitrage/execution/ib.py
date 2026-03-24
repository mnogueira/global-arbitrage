"""Interactive Brokers execution broker backed by the shared IB connector."""

from __future__ import annotations

from global_arbitrage.connectors.ib import InteractiveBrokersConnector


class IBExecutionBroker(InteractiveBrokersConnector):
    """Thin alias that keeps execution imports parallel with MT5."""
