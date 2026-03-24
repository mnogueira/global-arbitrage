"""Base contracts for arbitrage strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod

from global_arbitrage.core.models import StrategyObservation


class ArbitrageStrategy(ABC):
    """Minimal strategy interface used by the scanner and backtester."""

    strategy_id: str
    strategy_name: str

    @abstractmethod
    def refresh(self) -> StrategyObservation:
        """Return the latest observation for this strategy instance."""

    @abstractmethod
    def history(self, *, period: str = "2y", interval: str = "1d") -> list[StrategyObservation]:
        """Build a historical observation stream for backtests."""
