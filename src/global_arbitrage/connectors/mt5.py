"""MetaTrader 5 market-data connector for live B3 and futures quotes."""

from __future__ import annotations

from datetime import timedelta
from math import ceil, isfinite
from typing import Any

import pandas as pd

from global_arbitrage.connectors.base import MarketDataConnector
from global_arbitrage.core.models import MarketQuote


def _parse_time_window(value: str) -> timedelta:
    amount = int(value[:-1])
    unit = value[-1].lower()
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    if unit == "w":
        return timedelta(weeks=amount)
    if unit == "y":
        return timedelta(days=365 * amount)
    raise ValueError(f"Unsupported time value '{value}'.")


def _bar_count(period: str, interval: str) -> int:
    total = _parse_time_window(period)
    bar = _parse_time_window(interval)
    return max(2, min(10_000, ceil(total / bar) + 5))


def _first_finite(*values: object) -> float | None:
    for value in values:
        if value in {None, ""}:
            continue
        resolved = float(value)
        if isfinite(resolved):
            return resolved
    return None


class MT5Connector(MarketDataConnector):
    """Live quote and history adapter backed by MetaTrader 5."""

    venue = "mt5"

    def __init__(
        self,
        *,
        login: int | None = None,
        password: str | None = None,
        server: str | None = None,
        mt5_path: str | None = None,
        default_currency: str = "BRL",
        symbol_aliases: dict[str, str] | None = None,
    ):
        self.login = login
        self.password = password
        self.server = server
        self.mt5_path = mt5_path
        self.default_currency = default_currency
        self.symbol_aliases = dict(symbol_aliases or {})
        self._mt5: Any | None = None

    def connect(self) -> None:
        mt5 = self._import_mt5()
        kwargs: dict[str, object] = {}
        if self.mt5_path:
            kwargs["path"] = self.mt5_path
        if self.login is not None:
            kwargs["login"] = self.login
        if self.password:
            kwargs["password"] = self.password
        if self.server:
            kwargs["server"] = self.server
        if not mt5.initialize(**kwargs):
            raise ConnectionError(f"MT5 initialization failed: {mt5.last_error()}")
        self._mt5 = mt5

    def disconnect(self) -> None:
        if self._mt5 is not None:
            self._mt5.shutdown()
        self._mt5 = None

    def latest_quote(self, symbol: str, *, currency: str | None = None) -> MarketQuote:
        mt5 = self._require_mt5()
        resolved_symbol = self.resolve_symbol(symbol)
        self._ensure_symbol(resolved_symbol)
        info = mt5.symbol_info(resolved_symbol)
        tick = mt5.symbol_info_tick(resolved_symbol)
        if info is None or tick is None:
            raise RuntimeError(f"MT5 quote unavailable for '{resolved_symbol}'.")
        bid = _first_finite(getattr(tick, "bid", None))
        ask = _first_finite(getattr(tick, "ask", None))
        midpoint = None if bid is None or ask is None else (bid + ask) / 2.0
        last = _first_finite(getattr(tick, "last", None), midpoint, getattr(info, "last", None))
        if last is None:
            raise RuntimeError(f"MT5 returned no usable price fields for '{resolved_symbol}'.")
        timestamp_value = getattr(tick, "time_msc", None)
        if timestamp_value not in {None, 0}:
            timestamp = pd.Timestamp(int(timestamp_value), unit="ms", tz="UTC").tz_localize(None)
        else:
            timestamp = pd.Timestamp(int(getattr(tick, "time", 0)), unit="s", tz="UTC").tz_localize(
                None
            )
        resolved_currency = (
            currency
            or str(getattr(info, "currency_profit", "") or getattr(info, "currency_base", ""))
            or self.default_currency
        )
        return MarketQuote(
            venue=self.venue,
            symbol=resolved_symbol,
            last=last,
            bid=bid,
            ask=ask,
            currency=resolved_currency,
            timestamp=timestamp,
            source="MetaTrader5 tick",
            metadata={"requested_symbol": symbol},
        )

    def history(
        self,
        symbol: str,
        *,
        period: str = "2y",
        interval: str = "1d",
        currency: str | None = None,
    ) -> pd.DataFrame:
        mt5 = self._require_mt5()
        resolved_symbol = self.resolve_symbol(symbol)
        self._ensure_symbol(resolved_symbol)
        timeframe = self._resolve_timeframe(mt5, interval)
        payload = mt5.copy_rates_from_pos(resolved_symbol, timeframe, 0, _bar_count(period, interval))
        if payload is None or len(payload) == 0:
            raise ValueError(f"MT5 returned no history for '{resolved_symbol}'.")
        frame = pd.DataFrame(payload)
        frame["timestamp"] = pd.to_datetime(frame["time"], unit="s", utc=True).dt.tz_localize(None)
        frame = frame.set_index("timestamp")
        frame["volume"] = frame["real_volume"].where(frame["real_volume"] > 0, frame["tick_volume"])
        frame["currency"] = currency or self.default_currency
        return frame[["open", "high", "low", "close", "volume", "currency"]]

    def resolve_symbol(self, symbol: str) -> str:
        return self.symbol_aliases.get(symbol, symbol)

    def _ensure_symbol(self, symbol: str) -> None:
        mt5 = self._require_mt5()
        info = mt5.symbol_info(symbol)
        if info is None:
            raise ValueError(f"MT5 symbol '{symbol}' was not found.")
        if not info.visible:
            mt5.symbol_select(symbol, True)

    def _require_mt5(self):
        if self._mt5 is None:
            self.connect()
        if self._mt5 is None:
            raise RuntimeError("MT5 connector could not initialize.")
        return self._mt5

    @staticmethod
    def _resolve_timeframe(mt5: Any, interval: str):
        mapping = {
            "1m": mt5.TIMEFRAME_M1,
            "5m": mt5.TIMEFRAME_M5,
            "15m": mt5.TIMEFRAME_M15,
            "30m": mt5.TIMEFRAME_M30,
            "1h": mt5.TIMEFRAME_H1,
            "4h": mt5.TIMEFRAME_H4,
            "1d": mt5.TIMEFRAME_D1,
            "1w": mt5.TIMEFRAME_W1,
        }
        if interval not in mapping:
            raise ValueError(f"Unsupported MT5 interval '{interval}'.")
        return mapping[interval]

    @staticmethod
    def _import_mt5():
        try:
            import MetaTrader5 as mt5  # noqa: N813

            return mt5
        except ImportError as exc:
            raise ImportError(
                "MetaTrader5 package is required for MT5 market data. Install with: uv sync --extra mt5"
            ) from exc
