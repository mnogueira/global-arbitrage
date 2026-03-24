"""Official and fallback FX connectors."""

from __future__ import annotations

from datetime import date, timedelta
from typing import TYPE_CHECKING

import pandas as pd
import requests

from global_arbitrage.connectors.yahoo import YahooFinanceConnector
from global_arbitrage.core.models import MarketQuote

if TYPE_CHECKING:
    from global_arbitrage.connectors.base import MarketDataConnector


def _resolve_usdbrl_inversion(reference_series: pd.Series) -> bool:
    """Determine whether a Yahoo FX series must be inverted to become USD/BRL."""

    median = float(reference_series.astype(float).dropna().median())
    if 2.0 <= median <= 15.0:
        return False
    if 0.05 <= median <= 0.5:
        return True
    raise ValueError(
        "Unexpected Yahoo FX convention for USD/BRL. "
        f"Median was {median:.6f}; expected direct USD/BRL in [2, 15] or inverse in [0.05, 0.5]."
    )


def normalize_usdbrl_series(series: pd.Series, *, invert: bool = False) -> pd.Series:
    """Normalize one series into USD/BRL after the orientation is known."""

    cleaned = series.astype(float)
    normalized = 1.0 / cleaned if invert else cleaned
    median = float(normalized.dropna().median())
    if not 2.0 <= median <= 15.0:
        raise ValueError(
            f"Normalized USD/BRL median {median:.6f} is outside the expected [2, 15] range."
        )
    return normalized


def normalize_usdbrl_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize OHLC Yahoo FX data using one inversion decision for the whole frame."""

    invert = _resolve_usdbrl_inversion(frame["close"])
    normalized = frame.copy()
    for column in ("open", "high", "low", "close"):
        normalized[column] = normalize_usdbrl_series(normalized[column], invert=invert)
    return normalized


class BcbPtaxConnector:
    """FX connector with broker-native live proxy plus PTAX and Yahoo fallbacks."""

    BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"

    def __init__(
        self,
        *,
        yahoo: YahooFinanceConnector | None = None,
        market: "MarketDataConnector" | None = None,
        market_symbol: str | None = None,
        market_scale: float = 1.0,
        prefer_market_proxy: bool = False,
    ):
        self.yahoo = yahoo or YahooFinanceConnector()
        self.market = market
        self.market_symbol = market_symbol
        self.market_scale = float(market_scale)
        self.prefer_market_proxy = prefer_market_proxy

    def latest_usdbrl(self, reference_date: date | None = None) -> MarketQuote:
        """Fetch the latest USD/BRL quote with a broker-native proxy when configured."""

        fetchers = [self._latest_from_market_proxy, lambda: self._latest_from_bcb(reference_date)]
        if not self.prefer_market_proxy:
            fetchers.reverse()
        last_error: Exception | None = None
        for fetcher in fetchers:
            try:
                return fetcher()
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise ValueError("No USD/BRL quote source is configured.")

    def history_usdbrl(self, *, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
        """Return USD/BRL history from a broker-native proxy or Yahoo fallback."""

        fetchers = [lambda: self._history_from_market_proxy(period=period, interval=interval)]
        if self.yahoo is not None:
            fetchers.append(lambda: self._history_from_yahoo(period=period, interval=interval))
        if not self.prefer_market_proxy:
            fetchers.reverse()
        last_error: Exception | None = None
        for fetcher in fetchers:
            try:
                return fetcher()
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise ValueError("No USD/BRL history source is configured.")

    def _latest_from_bcb(self, reference_date: date | None) -> MarketQuote:
        target = reference_date or date.today()
        for offset in range(0, 7):
            probe = target - timedelta(days=offset)
            payload = self._fetch_day(probe)
            values = payload.get("value", [])
            if not values:
                continue
            row = values[-1]
            timestamp = pd.Timestamp(row["dataHoraCotacao"]).tz_localize(None)
            return MarketQuote(
                venue="bcb",
                symbol="USD/BRL",
                last=(float(row["cotacaoCompra"]) + float(row["cotacaoVenda"])) / 2.0,
                bid=float(row["cotacaoCompra"]),
                ask=float(row["cotacaoVenda"]),
                currency="BRL",
                timestamp=timestamp,
                source="BCB PTAX",
            )
        raise ValueError("BCB PTAX returned no recent USD/BRL quotes.")

    def _history_from_yahoo(self, *, period: str, interval: str) -> pd.DataFrame:
        frame = self.yahoo.history("BRL=X", period=period, interval=interval, currency="BRL")
        normalized = normalize_usdbrl_frame(frame)
        normalized["currency"] = "BRL"
        return normalized

    def _latest_from_market_proxy(self) -> MarketQuote:
        if self.market is None or not self.market_symbol:
            raise ValueError("No broker-native FX proxy is configured.")
        if self.market_scale <= 0.0:
            raise ValueError("FX proxy scale must be positive.")
        quote = self.market.latest_quote(self.market_symbol, currency="BRL")
        return MarketQuote(
            venue=quote.venue,
            symbol="USD/BRL",
            last=float(quote.last) / self.market_scale,
            bid=None if quote.bid is None else float(quote.bid) / self.market_scale,
            ask=None if quote.ask is None else float(quote.ask) / self.market_scale,
            currency="BRL",
            timestamp=quote.timestamp,
            source=f"{quote.source or quote.venue}:{self.market_symbol}",
            metadata={
                **quote.metadata,
                "proxy_symbol": self.market_symbol,
                "proxy_scale": self.market_scale,
            },
        )

    def _history_from_market_proxy(self, *, period: str, interval: str) -> pd.DataFrame:
        if self.market is None or not self.market_symbol:
            raise ValueError("No broker-native FX proxy is configured.")
        if self.market_scale <= 0.0:
            raise ValueError("FX proxy scale must be positive.")
        frame = self.market.history(self.market_symbol, period=period, interval=interval, currency="BRL")
        normalized = frame.copy()
        for column in ("open", "high", "low", "close"):
            normalized[column] = normalized[column].astype(float) / self.market_scale
        normalized["currency"] = "BRL"
        return normalized

    def _fetch_day(self, probe: date) -> dict[str, object]:
        formatted = probe.strftime("%m-%d-%Y")
        url = (
            f"{self.BASE_URL}/CotacaoMoedaDia(moeda=@moeda,dataCotacao=@dataCotacao)"
            f"?@moeda='USD'&@dataCotacao='{formatted}'&$top=100&$format=json"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
