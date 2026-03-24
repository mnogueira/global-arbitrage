import pandas as pd

from global_arbitrage.core.costs import CostAssumptions
from global_arbitrage.core.models import MarketQuote, SignalSide
from global_arbitrage.strategies.crypto_implied_fx import CryptoImpliedFxStrategy


class FakeYahoo:
    def history(self, symbol: str, *, period: str = "2y", interval: str = "1d", currency: str | None = None) -> pd.DataFrame:
        index = pd.date_range("2026-03-20", periods=3, freq="D")
        if symbol == "BTC-BRL":
            close = 480000.0
        elif symbol == "BTC-USD":
            close = 100000.0
        else:
            raise KeyError(symbol)
        return pd.DataFrame({"open": close, "high": close, "low": close, "close": close}, index=index)


class FakeBinance:
    def latest_quote(self, symbol: str, *, currency: str | None = None) -> MarketQuote:
        price = 480000.0 if symbol == "BTCBRL" else 100000.0
        return MarketQuote(
            venue="fake",
            symbol=symbol,
            last=price,
            bid=price,
            ask=price,
            currency=currency or "USD",
            timestamp=pd.Timestamp("2026-03-24"),
        )


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
        index = pd.date_range("2026-03-20", periods=3, freq="D")
        return pd.DataFrame(
            {"open": 5.0, "high": 5.0, "low": 5.0, "close": 5.0, "currency": "BRL"},
            index=index,
        )


class FakeBitso:
    def latest_quote(self, book: str) -> MarketQuote:
        return MarketQuote(
            venue="fake",
            symbol=book,
            last=2000000.0,
            bid=2000000.0,
            ask=2000000.0,
            currency=book.split("_")[-1].upper(),
            timestamp=pd.Timestamp("2026-03-24"),
        )


def test_crypto_strategy_signals_long_when_brl_btc_is_cheap() -> None:
    strategy = CryptoImpliedFxStrategy(
        strategy_id="crypto_test",
        local_symbol="BTCBRL",
        usd_symbol="BTCUSDT",
        yahoo=FakeYahoo(),
        fx=FakeFx(),
        binance=FakeBinance(),
        bitso=FakeBitso(),
        costs=CostAssumptions(exchange_fee_bps=10.0),
        open_threshold_bps=20.0,
        close_threshold_bps=5.0,
        max_holding_bars=3,
        capital_required_brl=100000.0,
        bitso_books=("btc_mxn",),
    )
    observation = strategy.refresh()
    assert observation.signal is SignalSide.LONG
    assert observation.net_edge_bps > 0.0
