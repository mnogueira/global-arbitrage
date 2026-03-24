"""Binance public spot-market connector."""

from __future__ import annotations

import pandas as pd
import requests

from global_arbitrage.connectors.base import MarketDataConnector
from global_arbitrage.core.models import MarketQuote


class BinanceSpotConnector(MarketDataConnector):
    """Read public ticker and kline data from Binance."""

    BASE_URL = "https://api.binance.com/api/v3"

    def latest_quote(self, symbol: str, *, currency: str | None = None) -> MarketQuote:
        response = requests.get(
            f"{self.BASE_URL}/ticker/bookTicker",
            params={"symbol": symbol.upper()},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        bid = float(payload["bidPrice"])
        ask = float(payload["askPrice"])
        return MarketQuote(
            venue="binance",
            symbol=symbol.upper(),
            last=(bid + ask) / 2.0,
            bid=bid,
            ask=ask,
            currency=currency or self._infer_currency(symbol),
            timestamp=pd.Timestamp.utcnow().tz_localize(None),
            source="Binance bookTicker",
        )

    def history(
        self,
        symbol: str,
        *,
        period: str = "365d",
        interval: str = "1d",
        currency: str | None = None,
    ) -> pd.DataFrame:
        limit = 365 if interval.endswith("d") else 500
        response = requests.get(
            f"{self.BASE_URL}/klines",
            params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload:
            raise ValueError(f"Binance returned no history for '{symbol}'.")
        frame = pd.DataFrame(
            payload,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_asset_volume",
                "trade_count",
                "taker_buy_base",
                "taker_buy_quote",
                "ignore",
            ],
        )
        frame["timestamp"] = pd.to_datetime(frame["open_time"], unit="ms", utc=True).dt.tz_localize(
            None
        )
        frame = frame.set_index("timestamp")
        for column in ("open", "high", "low", "close", "volume"):
            frame[column] = frame[column].astype(float)
        frame["currency"] = currency or self._infer_currency(symbol)
        return frame[["open", "high", "low", "close", "volume", "currency"]]

    @staticmethod
    def _infer_currency(symbol: str) -> str:
        upper = symbol.upper()
        if upper.endswith("BRL"):
            return "BRL"
        if upper.endswith("USDT") or upper.endswith("USD"):
            return "USD"
        return "USD"
