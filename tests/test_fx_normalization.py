import pandas as pd
import pytest

from global_arbitrage.connectors.fx import normalize_usdbrl_frame, normalize_usdbrl_series


def test_normalize_usdbrl_frame_keeps_direct_quotes() -> None:
    frame = pd.DataFrame(
        {
            "open": [5.1, 5.2],
            "high": [5.2, 5.3],
            "low": [5.0, 5.1],
            "close": [5.1, 5.2],
        }
    )
    normalized = normalize_usdbrl_frame(frame)
    assert normalized["close"].tolist() == [5.1, 5.2]


def test_normalize_usdbrl_frame_inverts_once_for_all_columns() -> None:
    frame = pd.DataFrame(
        {
            "open": [0.20, 0.19],
            "high": [0.21, 0.20],
            "low": [0.19, 0.18],
            "close": [0.20, 0.19],
        }
    )
    normalized = normalize_usdbrl_frame(frame)
    assert round(float(normalized["close"].iloc[0]), 4) == 5.0
    assert round(float(normalized["open"].iloc[1]), 4) == round(1.0 / 0.19, 4)


def test_normalize_usdbrl_series_rejects_unexpected_convention() -> None:
    with pytest.raises(ValueError, match="outside the expected"):
        normalize_usdbrl_series(pd.Series([1.1, 1.2, 1.3]))


def test_market_proxy_history_can_be_scaled_into_usdbrl() -> None:
    from global_arbitrage.connectors.fx import BcbPtaxConnector

    class FakeMarket:
        def history(
            self,
            symbol: str,
            *,
            period: str = "2y",
            interval: str = "1d",
            currency: str | None = None,
        ) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "open": [5100.0, 5200.0],
                    "high": [5150.0, 5250.0],
                    "low": [5050.0, 5150.0],
                    "close": [5125.0, 5225.0],
                    "currency": ["BRL", "BRL"],
                },
                index=pd.date_range("2026-03-01", periods=2, freq="D"),
            )

    connector = BcbPtaxConnector(
        market=FakeMarket(),
        market_symbol="WDO$N",
        market_scale=1000.0,
        prefer_market_proxy=True,
    )

    history = connector.history_usdbrl(period="1mo", interval="1d")

    assert history["close"].tolist() == [5.125, 5.225]
    assert set(history["currency"]) == {"BRL"}
