import pandas as pd

from global_arbitrage.core.costs import CostAssumptions
from global_arbitrage.core.models import MarketQuote, SignalSide
from global_arbitrage.strategies.adr_parity import ADRParityStrategy


class FakeMarket:
    def __init__(self, latest_quotes: dict[str, float], history_frames: dict[str, pd.DataFrame]):
        self.latest_quotes = latest_quotes
        self.history_frames = history_frames

    def latest_quote(self, symbol: str, *, currency: str | None = None) -> MarketQuote:
        return MarketQuote(
            venue="fake",
            symbol=symbol,
            last=self.latest_quotes[symbol],
            currency=currency or "BRL",
            timestamp=pd.Timestamp("2026-03-24"),
        )

    def history(
        self,
        symbol: str,
        *,
        period: str = "2y",
        interval: str = "1d",
        currency: str | None = None,
    ) -> pd.DataFrame:
        return self.history_frames[symbol]


class FakeFx:
    def latest_usdbrl(self) -> MarketQuote:
        return MarketQuote(
            venue="fake",
            symbol="USD/BRL",
            last=5.0,
            bid=4.99,
            ask=5.01,
            currency="BRL",
            timestamp=pd.Timestamp("2026-03-24"),
        )

    def history_usdbrl(self, *, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
        index = pd.date_range("2026-03-20", periods=3, freq="D")
        return pd.DataFrame(
            {"open": 5.0, "high": 5.0, "low": 5.0, "close": 5.0, "currency": "BRL"},
            index=index,
        )


def test_adr_strategy_signals_long_when_local_is_cheap() -> None:
    index = pd.date_range("2026-03-20", periods=3, freq="D")
    local_market = FakeMarket(
        latest_quotes={"PETR4": 9.0},
        history_frames={"PETR4": pd.DataFrame({"open": 9.0, "high": 9.0, "low": 9.0, "close": 9.0}, index=index)},
    )
    adr_market = FakeMarket(
        latest_quotes={"PBR": 4.0},
        history_frames={"PBR": pd.DataFrame({"open": 4.0, "high": 4.0, "low": 4.0, "close": 4.0}, index=index)},
    )
    strategy = ADRParityStrategy(
        strategy_id="adr_test",
        local_symbol="PETR4.SA",
        adr_symbol="PBR",
        local_name="Petrobras PN",
        shares_per_adr=2.0,
        local_market=local_market,
        adr_market=adr_market,
        fx=FakeFx(),
        costs=CostAssumptions(exchange_fee_bps=10.0),
        open_threshold_bps=50.0,
        close_threshold_bps=10.0,
        max_holding_bars=5,
        capital_required_brl=100000.0,
        local_market_symbol="PETR4",
        mt5_symbol="PETR4",
        ib_symbol="PBR",
    )
    observation = strategy.refresh()
    assert observation.signal is SignalSide.LONG
    assert observation.gross_spread_bps > 0.0
    assert observation.should_open
    assert observation.trade_legs[0].broker_venue == "mt5"
    assert observation.trade_legs[0].order_quantity_multiplier == 2.0
    assert observation.trade_legs[1].broker_venue == "ib"
