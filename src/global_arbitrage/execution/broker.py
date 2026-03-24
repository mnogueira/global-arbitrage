"""Shared execution-broker contracts and normalized broker state."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

import pandas as pd


class OrderSide(StrEnum):
    """Canonical order side."""

    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class OrderReceipt:
    """Normalized order acknowledgement from any broker."""

    symbol: str
    side: str
    quantity: float
    status: str
    venue: str
    order_id: str | None = None
    filled_price: float | None = None
    message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BrokerPosition:
    """Normalized position snapshot for one broker-held instrument."""

    venue: str
    symbol: str
    quantity: float
    currency: str
    average_price: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BrokerAccountSnapshot:
    """High-level account and P&L snapshot for one broker."""

    venue: str
    currency: str
    timestamp: pd.Timestamp
    account_id: str | None = None
    balance: float | None = None
    equity: float | None = None
    available_funds: float | None = None
    buying_power: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class ExecutionBroker(Protocol):
    """Protocol shared by MT5, IB, and future live brokers."""

    venue: str

    def connect(self) -> None:
        """Establish the broker session."""

    def disconnect(self) -> None:
        """Close the broker session."""

    def submit_market_order(self, *, symbol: str, side: OrderSide, quantity: float) -> OrderReceipt:
        """Submit a market order."""

    def positions(self) -> list[BrokerPosition]:
        """Return live broker positions."""

    def account_snapshot(self) -> BrokerAccountSnapshot:
        """Return a normalized account snapshot."""
