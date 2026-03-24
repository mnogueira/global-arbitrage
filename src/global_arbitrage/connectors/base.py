"""Abstract connector contracts."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from global_arbitrage.core.models import MarketQuote


class MarketDataConnector(ABC):
    """Minimal quote/history interface used by strategies."""

    @abstractmethod
    def latest_quote(self, symbol: str, *, currency: str | None = None) -> MarketQuote:
        """Return the latest available quote for one symbol."""

    @abstractmethod
    def history(
        self,
        symbol: str,
        *,
        period: str = "2y",
        interval: str = "1d",
        currency: str | None = None,
    ) -> pd.DataFrame:
        """Return an OHLCV-like historical frame."""
