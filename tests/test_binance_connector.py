import pandas as pd

from global_arbitrage.connectors.binance import BinanceSpotConnector


class FakeResponse:
    def raise_for_status(self) -> None:
        return None

    def json(self):
        return [
            [
                1_700_000_000_000,
                "100.0",
                "101.0",
                "99.0",
                "100.5",
                "10.0",
                1_700_000_060_000,
                "1000.0",
                100,
                "5.0",
                "500.0",
                "0",
            ]
        ]


def test_binance_history_uses_open_time_index(monkeypatch) -> None:
    monkeypatch.setattr("global_arbitrage.connectors.binance.requests.get", lambda *args, **kwargs: FakeResponse())
    connector = BinanceSpotConnector()
    frame = connector.history("BTCBRL", interval="1m")
    assert frame.index[0] == pd.Timestamp(1_700_000_000_000, unit="ms")
