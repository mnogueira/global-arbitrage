import pandas as pd

from global_arbitrage.core.models import TradeLeg
from global_arbitrage.execution.broker import BrokerAccountSnapshot, OrderReceipt, OrderSide
from global_arbitrage.execution.router import BrokerRouter


class StubBroker:
    def __init__(self, venue: str, currency: str, equity: float, unrealized: float, realized: float):
        self.venue = venue
        self.currency = currency
        self.equity = equity
        self.unrealized = unrealized
        self.realized = realized
        self.orders: list[tuple[str, OrderSide, float]] = []

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def submit_market_order(self, *, symbol: str, side: OrderSide, quantity: float) -> OrderReceipt:
        self.orders.append((symbol, side, quantity))
        return OrderReceipt(
            symbol=symbol,
            side=side.value,
            quantity=quantity,
            status="FILLED",
            venue=self.venue,
            order_id=f"{self.venue}-1",
            filled_price=100.0,
        )

    def positions(self):
        return []

    def account_snapshot(self) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            venue=self.venue,
            currency=self.currency,
            timestamp=pd.Timestamp("2026-03-24 14:00:00"),
            equity=self.equity,
            unrealized_pnl=self.unrealized,
            realized_pnl=self.realized,
        )


def test_broker_router_routes_by_leg_venue_and_combines_brl_pnl() -> None:
    mt5 = StubBroker("mt5", "BRL", equity=100000.0, unrealized=500.0, realized=200.0)
    ib = StubBroker("ib", "USD", equity=10000.0, unrealized=100.0, realized=50.0)
    router = BrokerRouter(
        brokers={"mt5": mt5, "ib": ib},
        default_order_quantities={"mt5": 100.0, "ib": 50.0},
    )
    trade_legs = (
        TradeLeg(
            instrument_id="b3:PETR4",
            display_name="PETR4",
            price=30.0,
            currency="BRL",
            direction=1,
            broker_symbol="PETR4",
            broker_venue="mt5",
            order_quantity_multiplier=2.0,
        ),
        TradeLeg(
            instrument_id="synthetic:PBR:translated",
            display_name="PBR translated",
            price=32.0,
            currency="BRL",
            direction=-1,
            broker_symbol="PBR",
            broker_venue="ib",
            order_quantity_multiplier=1.0,
        ),
    )

    receipts = router.execute_trade_legs(trade_legs, open_trade=True)
    snapshot = router.combined_account_snapshot(usdbrl=5.0)

    assert len(receipts) == 2
    assert mt5.orders == [("PETR4", OrderSide.BUY, 200.0)]
    assert ib.orders == [("PBR", OrderSide.SELL, 50.0)]
    assert snapshot.equity_brl == 150000.0
    assert snapshot.unrealized_pnl_brl == 1000.0
    assert snapshot.realized_pnl_brl == 450.0
