from types import SimpleNamespace

import pandas as pd

from global_arbitrage.connectors.mt5 import MT5Connector


class FakeMt5:
    TIMEFRAME_M1 = 1
    TIMEFRAME_D1 = 2

    def __init__(self) -> None:
        self.visible = False

    def symbol_info(self, symbol: str):
        if symbol != "PETR4":
            return None
        return SimpleNamespace(
            visible=self.visible,
            currency_profit="BRL",
            currency_base="BRL",
            bid=47.34 if self.visible else 0.0,
            ask=47.36 if self.visible else 0.0,
            last=47.36 if self.visible else 0.0,
            time=1774364483 if self.visible else 0,
        )

    def symbol_select(self, symbol: str, visible: bool) -> bool:
        self.visible = visible
        return True

    def symbol_info_tick(self, symbol: str):
        if symbol != "PETR4":
            return None
        return SimpleNamespace(bid=0.0, ask=0.0, last=0.0, time=0, time_msc=0)

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int):
        return [
            {
                "time": 1774364460,
                "open": 46.05,
                "high": 46.07,
                "low": 46.05,
                "close": 46.07,
            }
        ]

    def last_error(self):
        return (0, "ok")


class FakeMt5WithBarFallback(FakeMt5):
    def symbol_info(self, symbol: str):
        if symbol != "WIN$N":
            return None
        return SimpleNamespace(
            visible=True,
            currency_profit="BRL",
            currency_base="BRL",
            bid=0.0,
            ask=0.0,
            last=0.0,
            time=0,
        )

    def symbol_select(self, symbol: str, visible: bool) -> bool:
        return True

    def symbol_info_tick(self, symbol: str):
        if symbol != "WIN$N":
            return None
        return SimpleNamespace(bid=0.0, ask=0.0, last=0.0, time=0, time_msc=0)

    def copy_rates_from_pos(self, symbol: str, timeframe: int, start_pos: int, count: int):
        return [
            {
                "time": 1774364520,
                "open": 182945.0,
                "high": 182945.0,
                "low": 182820.0,
                "close": 182825.0,
            }
        ]


def test_mt5_connector_refreshes_symbol_selection_before_using_quote() -> None:
    connector = MT5Connector()
    connector._mt5 = FakeMt5()  # type: ignore[assignment]

    quote = connector.latest_quote("PETR4")

    assert quote.symbol == "PETR4"
    assert quote.bid == 47.34
    assert quote.ask == 47.36
    assert quote.last == 47.36
    assert quote.timestamp == pd.Timestamp("2026-03-24 18:34:43")


def test_mt5_connector_falls_back_to_recent_bar_close_when_ticks_are_zero() -> None:
    connector = MT5Connector()
    connector._mt5 = FakeMt5WithBarFallback()  # type: ignore[assignment]

    quote = connector.latest_quote("WIN$N")

    assert quote.symbol == "WIN$N"
    assert quote.bid is None
    assert quote.ask is None
    assert quote.last == 182825.0
    assert quote.timestamp == pd.Timestamp("2026-03-24 18:35:20")
