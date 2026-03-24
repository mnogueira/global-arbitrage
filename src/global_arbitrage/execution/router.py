"""Route multi-leg orders across brokers and aggregate broker P&L."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd

from global_arbitrage.core.models import TradeLeg
from global_arbitrage.execution.broker import (
    BrokerAccountSnapshot,
    ExecutionBroker,
    OrderReceipt,
    OrderSide,
)


@dataclass(frozen=True, slots=True)
class CombinedBrokerSnapshot:
    """Combined account snapshot across all configured brokers."""

    timestamp: pd.Timestamp
    snapshots: tuple[BrokerAccountSnapshot, ...]
    equity_brl: float | None
    unrealized_pnl_brl: float | None
    realized_pnl_brl: float | None


class BrokerRouter:
    """Route live orders to the correct broker and combine account state."""

    def __init__(
        self,
        *,
        brokers: dict[str, ExecutionBroker],
        default_order_quantities: dict[str, float] | None = None,
    ):
        self.brokers = dict(brokers)
        self.default_order_quantities = dict(default_order_quantities or {})

    def connect_all(self) -> None:
        for broker in self.brokers.values():
            broker.connect()

    def disconnect_all(self) -> None:
        for broker in self.brokers.values():
            broker.disconnect()

    def execute_trade_legs(self, trade_legs: tuple[TradeLeg, ...], *, open_trade: bool) -> tuple[OrderReceipt, ...]:
        receipts: list[OrderReceipt] = []
        for leg in trade_legs:
            if leg.broker_venue is None or leg.broker_symbol is None:
                continue
            broker = self.brokers.get(leg.broker_venue)
            if broker is None:
                raise KeyError(f"No broker configured for venue '{leg.broker_venue}'.")
            base_quantity = self.default_order_quantities.get(leg.broker_venue)
            if base_quantity is None or base_quantity <= 0.0:
                continue
            quantity = float(base_quantity) * abs(float(leg.order_quantity_multiplier))
            if quantity <= 0.0:
                continue
            side = self._side_for_leg(leg, open_trade=open_trade)
            receipts.append(
                broker.submit_market_order(
                    symbol=str(leg.broker_symbol),
                    side=side,
                    quantity=quantity,
                )
            )
        return tuple(receipts)

    def combined_account_snapshot(self, *, usdbrl: float | None = None) -> CombinedBrokerSnapshot:
        snapshots = tuple(broker.account_snapshot() for broker in self.brokers.values())
        return CombinedBrokerSnapshot(
            timestamp=pd.Timestamp.utcnow().tz_localize(None),
            snapshots=snapshots,
            equity_brl=self._sum_in_brl(
                ((snapshot.equity, snapshot.currency) for snapshot in snapshots),
                usdbrl,
            ),
            unrealized_pnl_brl=self._sum_in_brl(
                ((snapshot.unrealized_pnl, snapshot.currency) for snapshot in snapshots),
                usdbrl,
            ),
            realized_pnl_brl=self._sum_in_brl(
                ((snapshot.realized_pnl, snapshot.currency) for snapshot in snapshots),
                usdbrl,
            ),
        )

    @staticmethod
    def _side_for_leg(leg: TradeLeg, *, open_trade: bool) -> OrderSide:
        if open_trade:
            return OrderSide.BUY if leg.direction > 0 else OrderSide.SELL
        return OrderSide.SELL if leg.direction > 0 else OrderSide.BUY

    @staticmethod
    def _sum_in_brl(amounts: Iterable[tuple[float | None, str]], usdbrl: float | None) -> float | None:
        total = 0.0
        seen_any = False
        for amount, currency in amounts:
            if amount is None:
                continue
            if currency == "BRL":
                total += float(amount)
                seen_any = True
                continue
            if currency == "USD" and usdbrl is not None:
                total += float(amount) * float(usdbrl)
                seen_any = True
        return total if seen_any else None
