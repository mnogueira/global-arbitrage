"""Official and fallback FX connectors."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import requests

from global_arbitrage.connectors.yahoo import YahooFinanceConnector
from global_arbitrage.core.models import MarketQuote


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
    """Banco Central do Brasil PTAX with Yahoo fallback for history."""

    BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"

    def __init__(self, *, yahoo: YahooFinanceConnector | None = None):
        self.yahoo = yahoo or YahooFinanceConnector()

    def latest_usdbrl(self, reference_date: date | None = None) -> MarketQuote:
        """Fetch the latest PTAX quote, stepping back across non-business days."""

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

    def history_usdbrl(self, *, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
        """Use Yahoo history and normalize into USD/BRL terms."""

        frame = self.yahoo.history("BRL=X", period=period, interval=interval, currency="BRL")
        normalized = normalize_usdbrl_frame(frame)
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
