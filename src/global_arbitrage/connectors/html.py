"""Generic HTML scraper connector for simple public pages."""

from __future__ import annotations

import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

from global_arbitrage.core.models import MarketQuote


class HtmlValueConnector:
    """Extract a numeric value from an HTML element."""

    def latest_quote(
        self,
        *,
        url: str,
        css_selector: str,
        symbol: str,
        venue: str = "web",
        currency: str = "USD",
        regex: str = r"[-+]?\d+(?:[.,]\d+)?",
    ) -> MarketQuote:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        node = soup.select_one(css_selector)
        if node is None:
            raise ValueError(f"Selector '{css_selector}' not found on '{url}'.")
        match = re.search(regex, node.get_text(" ", strip=True))
        if match is None:
            raise ValueError(f"No numeric value found for selector '{css_selector}'.")
        token = match.group(0)
        value = float(token.replace(",", "")) if "." in token else float(token.replace(",", "."))
        return MarketQuote(
            venue=venue,
            symbol=symbol,
            last=value,
            currency=currency,
            timestamp=pd.Timestamp.utcnow().tz_localize(None),
            source=f"scrape:{url}",
        )
