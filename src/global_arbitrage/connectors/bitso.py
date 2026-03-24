"""Bitso public ticker connector for LatAm crypto checks."""

from __future__ import annotations

import pandas as pd
import requests

from global_arbitrage.core.models import MarketQuote


class BitsoConnector:
    """Read public ticker data from Bitso."""

    BASE_URL = "https://api.bitso.com/v3"

    def latest_quote(self, book: str) -> MarketQuote:
        response = requests.get(
            f"{self.BASE_URL}/ticker/",
            params={"book": book.lower()},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("success"):
            raise ValueError(f"Bitso request failed for '{book}'.")
        row = payload["payload"]
        return MarketQuote(
            venue="bitso",
            symbol=book.lower(),
            last=float(row["last"]),
            bid=float(row["bid"]),
            ask=float(row["ask"]),
            currency=book.split("_")[-1].upper(),
            timestamp=pd.Timestamp(row["created_at"]).tz_localize(None),
            source="Bitso ticker",
            metadata={"volume": row.get("volume")},
        )
