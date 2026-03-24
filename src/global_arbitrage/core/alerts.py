"""Alert sinks for large arbitrage signals."""

from __future__ import annotations

from typing import Protocol

from rich.console import Console

from global_arbitrage.core.models import StrategyObservation


class AlertSink(Protocol):
    """Protocol for alert backends."""

    def send(self, observation: StrategyObservation) -> None:
        """Emit one observation to an alert destination."""


class ConsoleAlertSink:
    """Rich console alert output."""

    def __init__(self, console: Console | None = None):
        self.console = console or Console()

    def send(self, observation: StrategyObservation) -> None:
        """Print a concise alert line for a strong signal."""

        direction = "LONG spread" if observation.signal.value > 0 else "SHORT spread"
        self.console.print(
            "[bold yellow]ALERT[/bold yellow] "
            f"{observation.strategy_id} {direction} "
            f"net={observation.net_edge_bps:.1f} bps "
            f"gross={observation.gross_spread_bps:.1f} bps "
            f"costs={observation.total_cost_bps:.1f} bps"
        )
