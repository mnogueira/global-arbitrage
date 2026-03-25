"""Interactive Brokers execution broker backed by the shared IB connector."""

from __future__ import annotations

from global_arbitrage.connectors.ib import InteractiveBrokersConnector


class IBExecutionBroker(InteractiveBrokersConnector):
    """IB execution broker that eagerly opens only the execution session."""

    def connect(self) -> None:
        self.connect_execution()
