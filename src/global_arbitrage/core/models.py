"""Shared models used across scanning, execution, and backtesting."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Any

import pandas as pd


class OpportunityState(StrEnum):
    """High-level opportunity status used in reports and alerts."""

    PASS_ = "pass"
    WATCH = "watch"
    OPEN = "open"


class SignalSide(IntEnum):
    """Canonical pair-trade direction."""

    SHORT = -1
    FLAT = 0
    LONG = 1


@dataclass(frozen=True, slots=True)
class MarketQuote:
    """Normalized market quote from any connector."""

    venue: str
    symbol: str
    last: float
    currency: str
    timestamp: pd.Timestamp
    bid: float | None = None
    ask: float | None = None
    source: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def mid(self) -> float:
        """Return the midpoint when possible, else the last price."""

        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2.0
        return self.last


@dataclass(frozen=True, slots=True)
class TradeLeg:
    """One leg of a synthetic pair or basis trade."""

    instrument_id: str
    display_name: str
    price: float
    currency: str
    direction: int
    weight: float = 1.0
    broker_symbol: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StrategyObservation:
    """One fully costed scanner output for a strategy instance."""

    strategy_id: str
    strategy_name: str
    timestamp: pd.Timestamp
    state: OpportunityState
    signal: SignalSide
    gross_spread_bps: float
    net_edge_bps: float
    fair_value: float
    market_price: float
    total_cost_bps: float
    capital_required_brl: float
    trade_legs: tuple[TradeLeg, ...]
    open_threshold_bps: float
    close_threshold_bps: float
    max_holding_bars: int
    notes: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def abs_net_edge_bps(self) -> float:
        """Absolute net edge after costs."""

        return abs(self.net_edge_bps)

    @property
    def should_open(self) -> bool:
        """Whether the observation is strong enough to open a position."""

        return self.signal is not SignalSide.FLAT and self.abs_net_edge_bps >= self.open_threshold_bps

    def to_record(self) -> dict[str, Any]:
        """Convert the observation to a storage-friendly record."""

        return {
            "strategy_id": self.strategy_id,
            "strategy_name": self.strategy_name,
            "timestamp": self.timestamp.isoformat(),
            "state": self.state.value,
            "signal": int(self.signal),
            "gross_spread_bps": self.gross_spread_bps,
            "net_edge_bps": self.net_edge_bps,
            "fair_value": self.fair_value,
            "market_price": self.market_price,
            "total_cost_bps": self.total_cost_bps,
            "capital_required_brl": self.capital_required_brl,
            "open_threshold_bps": self.open_threshold_bps,
            "close_threshold_bps": self.close_threshold_bps,
            "max_holding_bars": self.max_holding_bars,
            "notes": list(self.notes),
            "metadata": self.metadata,
            "trade_legs": [
                {
                    "instrument_id": leg.instrument_id,
                    "display_name": leg.display_name,
                    "price": leg.price,
                    "currency": leg.currency,
                    "direction": leg.direction,
                    "weight": leg.weight,
                    "broker_symbol": leg.broker_symbol,
                    "metadata": leg.metadata,
                }
                for leg in self.trade_legs
            ],
        }


@dataclass(frozen=True, slots=True)
class BacktestSummary:
    """Compact backtest summary used by CLI reporting."""

    strategy_id: str
    trades: int
    wins: int
    ending_equity_brl: float
    total_return_pct: float
    max_drawdown_pct: float
    avg_trade_pnl_brl: float
    win_rate_pct: float
