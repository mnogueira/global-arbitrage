"""Yahoo Finance connector used for broad public-market coverage."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from tenacity import retry, stop_after_attempt, wait_fixed

from global_arbitrage.connectors.base import MarketDataConnector
from global_arbitrage.core.models import MarketQuote
from global_arbitrage.core.utils import assert_timestamp_fresh

_CACHE_DIR = Path("storage") / "yfinance-cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
if hasattr(yf, "set_tz_cache_location"):
    yf.set_tz_cache_location(str(_CACHE_DIR))


def _normalize_history_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("Yahoo Finance returned an empty frame.")
    normalized = frame.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        normalized = normalized.droplevel(-1, axis=1)
    normalized.columns = [str(column).strip().lower() for column in normalized.columns]
    if "adj close" in normalized.columns and "close" not in normalized.columns:
        normalized["close"] = normalized["adj close"]
    expected = {"open", "high", "low", "close"}
    missing = expected - set(normalized.columns)
    if missing:
        raise ValueError(f"Yahoo Finance frame is missing required columns: {sorted(missing)}")
    normalized.index = pd.to_datetime(normalized.index).tz_localize(None)
    return normalized


class YahooFinanceConnector(MarketDataConnector):
    """Small wrapper around yfinance with normalized output."""

    def __init__(self, *, max_quote_age: timedelta = timedelta(days=5)):
        self.max_quote_age = max_quote_age

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(3), reraise=True)
    def latest_quote(self, symbol: str, *, currency: str | None = None) -> MarketQuote:
        frame = self.history(symbol, period="5d", interval="1h", currency=currency)
        row = frame.iloc[-1]
        timestamp = pd.Timestamp(frame.index[-1]).tz_localize(None)
        assert_timestamp_fresh(timestamp, max_age=self.max_quote_age)
        return MarketQuote(
            venue="yahoo",
            symbol=symbol,
            last=float(row["close"]),
            bid=None,
            ask=None,
            currency=currency or self._infer_currency(symbol),
            timestamp=timestamp,
            source="yfinance",
        )

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(3), reraise=True)
    def history(
        self,
        symbol: str,
        *,
        period: str = "2y",
        interval: str = "1d",
        currency: str | None = None,
    ) -> pd.DataFrame:
        frame = yf.download(
            tickers=symbol,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        normalized = _normalize_history_frame(frame)
        normalized["currency"] = currency or self._infer_currency(symbol)
        return normalized

    @staticmethod
    def _infer_currency(symbol: str) -> str:
        upper = symbol.upper()
        if upper.endswith(".SA"):
            return "BRL"
        if "BTC-BRL" in upper or ("BRL" in upper and "-BRL" in upper):
            return "BRL"
        if upper.endswith("=X"):
            return "FX"
        return "USD"
