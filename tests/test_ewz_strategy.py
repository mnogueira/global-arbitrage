import pandas as pd
import pytest

from global_arbitrage.core.costs import CostAssumptions
from global_arbitrage.core.models import MarketQuote, SignalSide
from global_arbitrage.strategies.ewz_leadlag import EwzBovaBridgeStrategy


class FakeMarket:
    def __init__(self, latest_quotes: dict[str, float], history_frames: dict[str, pd.DataFrame]):
        self.latest_quotes = latest_quotes
        self.history_frames = history_frames
        self.history_calls = 0

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
        self.history_calls += 1
        return self.history_frames[symbol]


class FakeFx:
    def latest_usdbrl(self) -> MarketQuote:
        return MarketQuote(
            venue="fake",
            symbol="USD/BRL",
            last=5.0,
            bid=5.0,
            ask=5.0,
            currency="BRL",
            timestamp=pd.Timestamp("2026-03-24"),
        )

    def history_usdbrl(self, *, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
        index = pd.date_range("2026-03-01", periods=10, freq="D")
        return pd.DataFrame(
            {"open": 5.0, "high": 5.0, "low": 5.0, "close": 5.0, "currency": "BRL"},
            index=index,
        )


def test_ewz_bridge_flags_bova_discount() -> None:
    index = pd.date_range("2026-03-01", periods=10, freq="D")
    local_market = FakeMarket(
        latest_quotes={"BOVA11": 95.0},
        history_frames={
            "BOVA11": pd.DataFrame(
                {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
                index=index,
            ),
        },
    )
    external_market = FakeMarket(
        latest_quotes={"EWZ": 20.0},
        history_frames={
            "EWZ": pd.DataFrame(
                {"open": 20.0, "high": 20.0, "low": 20.0, "close": 20.0},
                index=index,
            ),
        },
    )
    strategy = EwzBovaBridgeStrategy(
        strategy_id="ewz_test",
        local_symbol="BOVA11.SA",
        external_symbol="EWZ",
        lookback=3,
        local_market=local_market,
        external_market=external_market,
        fx=FakeFx(),
        costs=CostAssumptions(exchange_fee_bps=5.0),
        open_threshold_bps=20.0,
        close_threshold_bps=5.0,
        max_holding_bars=3,
        capital_required_brl=100000.0,
        mt5_symbol="BOVA11",
        ib_symbol="EWZ",
    )
    observation = strategy.refresh()
    assert observation.signal is SignalSide.LONG
    assert observation.fair_value > observation.market_price
    assert observation.trade_legs[1].broker_venue == "ib"


def test_ewz_bridge_caches_hedge_ratio_between_refresh_calls() -> None:
    index = pd.date_range("2026-03-01", periods=10, freq="D")
    local_market = FakeMarket(
        latest_quotes={"BOVA11": 95.0},
        history_frames={
            "BOVA11": pd.DataFrame(
                {"open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0},
                index=index,
            ),
        },
    )
    external_market = FakeMarket(
        latest_quotes={"EWZ": 20.0},
        history_frames={
            "EWZ": pd.DataFrame(
                {"open": 20.0, "high": 20.0, "low": 20.0, "close": 20.0},
                index=index,
            ),
        },
    )
    strategy = EwzBovaBridgeStrategy(
        strategy_id="ewz_cached",
        local_symbol="BOVA11.SA",
        external_symbol="EWZ",
        lookback=3,
        local_market=local_market,
        external_market=external_market,
        fx=FakeFx(),
        costs=CostAssumptions(exchange_fee_bps=5.0),
        open_threshold_bps=20.0,
        close_threshold_bps=5.0,
        max_holding_bars=3,
        capital_required_brl=100000.0,
        hedge_ratio_cache_ttl_seconds=60,
        mt5_symbol="BOVA11",
        ib_symbol="EWZ",
    )
    strategy.refresh()
    first_calls = local_market.history_calls + external_market.history_calls
    strategy.refresh()
    assert local_market.history_calls + external_market.history_calls == first_calls


def test_ewz_bridge_raises_when_no_valid_hedge_ratio_exists() -> None:
    index = pd.date_range("2026-03-01", periods=3, freq="D")
    local_market = FakeMarket(
        latest_quotes={"BOVA11": 95.0},
        history_frames={
            "BOVA11": pd.DataFrame(
                {
                    "open": [None, None, None],
                    "high": [None, None, None],
                    "low": [None, None, None],
                    "close": [None, None, None],
                },
                index=index,
            ),
        },
    )
    external_market = FakeMarket(
        latest_quotes={"EWZ": 20.0},
        history_frames={
            "EWZ": pd.DataFrame(
                {"open": 20.0, "high": 20.0, "low": 20.0, "close": 20.0},
                index=index,
            ),
        },
    )
    strategy = EwzBovaBridgeStrategy(
        strategy_id="ewz_guard",
        local_symbol="BOVA11.SA",
        external_symbol="EWZ",
        lookback=3,
        local_market=local_market,
        external_market=external_market,
        fx=FakeFx(),
        costs=CostAssumptions(exchange_fee_bps=5.0),
        open_threshold_bps=20.0,
        close_threshold_bps=5.0,
        max_holding_bars=3,
        capital_required_brl=100000.0,
        mt5_symbol="BOVA11",
        ib_symbol="EWZ",
    )
    with pytest.raises(ValueError, match="no valid rows"):
        strategy.refresh()
