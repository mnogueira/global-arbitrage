from datetime import timedelta

import pandas as pd
import pytest

from global_arbitrage.connectors.yahoo import YahooFinanceConnector


class StubYahooConnector(YahooFinanceConnector):
    def __init__(self) -> None:
        super().__init__(max_quote_age=timedelta(days=1))

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
                "open": [10.0],
                "high": [10.0],
                "low": [10.0],
                "close": [10.0],
                "currency": [currency or "USD"],
            },
            index=[pd.Timestamp("2020-03-20")],
        )


def test_latest_quote_rejects_stale_timestamp() -> None:
    connector = StubYahooConnector()
    with pytest.raises(ValueError, match="stale"):
        connector.latest_quote("EWZ")
