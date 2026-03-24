from dataclasses import dataclass
from types import SimpleNamespace

import pandas as pd

from global_arbitrage.connectors.ib import IBContractSpec, InteractiveBrokersConnector
from global_arbitrage.execution.broker import OrderSide


@dataclass
class FakeAccountValue:
    tag: str
    value: str
    currency: str


class FakeTicker:
    bid = 19.9
    ask = 20.1
    last = 20.0
    time = pd.Timestamp("2026-03-24 14:00:00")

    def marketPrice(self) -> float:
        return 20.0


class FakeTrade:
    def __init__(self) -> None:
        self.orderStatus = SimpleNamespace(status="Filled", avgFillPrice=20.05)
        self.order = SimpleNamespace(orderId=12345)

    def isDone(self) -> bool:
        return True


class FakeIB:
    def __init__(self) -> None:
        self.connected = False

    def connect(self, *args, **kwargs) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def reqMarketDataType(self, value: int) -> None:
        self.market_data_type = value

    def reqContractDetails(self, contract):
        return [SimpleNamespace(contract=contract)]

    def reqMktData(self, contract, *args):
        return FakeTicker()

    def reqTickers(self, contract):
        return [FakeTicker()]

    def reqHistoricalData(self, contract, **kwargs):
        return [
            {"date": pd.Timestamp("2026-03-20"), "open": 19.0, "high": 21.0, "low": 18.5, "close": 20.0, "volume": 1000.0},
            {"date": pd.Timestamp("2026-03-21"), "open": 20.0, "high": 21.5, "low": 19.5, "close": 20.5, "volume": 1200.0},
        ]

    def placeOrder(self, contract, order):
        return FakeTrade()

    def loopUntil(self, condition, timeout: float):
        yield condition()

    def managedAccounts(self):
        return ["DU123456"]

    def portfolio(self, account: str):
        return [
            SimpleNamespace(
                contract=SimpleNamespace(symbol="EWZ", currency="USD", exchange="SMART"),
                position=100.0,
                averageCost=19.5,
                marketPrice=20.0,
                marketValue=2000.0,
                unrealizedPNL=50.0,
                realizedPNL=10.0,
            )
        ]

    def accountSummary(self, account: str):
        return [
            FakeAccountValue("NetLiquidation", "10000", "BASE"),
            FakeAccountValue("AvailableFunds", "8000", "BASE"),
            FakeAccountValue("BuyingPower", "16000", "BASE"),
            FakeAccountValue("UnrealizedPnL", "200", "BASE"),
            FakeAccountValue("RealizedPnL", "50", "BASE"),
            FakeAccountValue("TotalCashValue", "9500", "BASE"),
        ]

    def sleep(self, seconds: float) -> None:
        return None


class EmptyTicker:
    bid = None
    ask = None
    last = None
    close = None
    time = pd.Timestamp("2026-03-24 14:00:00")

    def marketPrice(self) -> float:
        return float("nan")


class DelayedFallbackIB(FakeIB):
    def __init__(self) -> None:
        super().__init__()
        self.market_data_type = 1
        self.market_data_type_history: list[int] = []

    def reqMarketDataType(self, value: int) -> None:
        self.market_data_type = value
        self.market_data_type_history.append(value)

    def reqMktData(self, contract, *args):
        if self.market_data_type == 3:
            return FakeTicker()
        return EmptyTicker()

    def reqTickers(self, contract):
        if self.market_data_type == 3:
            return [FakeTicker()]
        return [EmptyTicker()]


class FakeModule:
    IB = FakeIB
    Contract = SimpleNamespace
    util = SimpleNamespace(df=lambda bars: pd.DataFrame(bars))

    class MarketOrder:
        def __init__(self, side: str, quantity: float):
            self.action = side
            self.totalQuantity = quantity


class DelayedFallbackModule(FakeModule):
    IB = DelayedFallbackIB


def test_ib_connector_supports_quotes_history_orders_and_account_state() -> None:
    connector = InteractiveBrokersConnector(
        contract_overrides={"EWZ": IBContractSpec(symbol="EWZ", sec_type="STK", exchange="SMART", currency="USD")}
    )
    connector._module = FakeModule()  # type: ignore[assignment]

    quote = connector.latest_quote("EWZ")
    history = connector.history("EWZ", period="5d", interval="1d")
    receipt = connector.submit_market_order(symbol="EWZ", side=OrderSide.BUY, quantity=10)
    positions = connector.positions()
    snapshot = connector.account_snapshot()

    assert quote.venue == "ib"
    assert round(quote.mid, 2) == 20.0
    assert history.iloc[-1]["close"] == 20.5
    assert receipt.status == "FILLED"
    assert receipt.filled_price == 20.05
    assert positions[0].symbol == "EWZ"
    assert snapshot.equity == 10000.0
    assert snapshot.currency == "USD"


def test_ib_connector_falls_back_to_delayed_market_data_when_live_quote_is_unavailable() -> None:
    connector = InteractiveBrokersConnector(
        contract_overrides={"EWZ": IBContractSpec(symbol="EWZ", sec_type="STK", exchange="SMART", currency="USD")}
    )
    connector._module = DelayedFallbackModule()  # type: ignore[assignment]

    quote = connector.latest_quote("EWZ")

    assert round(quote.mid, 2) == 20.0
    assert quote.metadata["requested_market_data_type"] == 1
    assert quote.metadata["market_data_type"] == 3
    assert connector._effective_market_data_type == 3
