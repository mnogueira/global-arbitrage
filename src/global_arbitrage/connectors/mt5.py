"""MetaTrader 5 market-data connector for live B3 and futures quotes."""

from __future__ import annotations

import time
from datetime import timedelta
from math import ceil, isfinite
from typing import Any

import pandas as pd

from global_arbitrage.connectors.base import MarketDataConnector
from global_arbitrage.core.models import MarketQuote


def _parse_time_window(value: str) -> timedelta:
    cleaned = value.strip().lower()
    if cleaned.endswith("mo"):
        amount = int(cleaned[:-2])
        unit = "mo"
    else:
        amount = int(cleaned[:-1])
        unit = cleaned[-1]
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "d":
        return timedelta(days=amount)
    if unit == "w":
        return timedelta(weeks=amount)
    if unit == "mo":
        return timedelta(days=30 * amount)
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


def _first_positive_price(*values: object) -> float | None:
    for value in values:
        resolved = _first_finite(value)
        if resolved is not None and resolved > 0.0:
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
        quote_poll_attempts: int = 3,
        quote_poll_interval_seconds: float = 0.2,
    ):
        self.login = login
        self.password = password
        self.server = server
        self.mt5_path = mt5_path
        self.default_currency = default_currency
        self.symbol_aliases = dict(symbol_aliases or {})
        self.quote_poll_attempts = max(1, int(quote_poll_attempts))
        self.quote_poll_interval_seconds = max(0.0, float(quote_poll_interval_seconds))
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
        info = self._ensure_symbol(resolved_symbol)
        tick = None
        recent_bar_close = None
        recent_bar_timestamp = None
        for attempt in range(self.quote_poll_attempts):
            info = mt5.symbol_info(resolved_symbol)
            tick = mt5.symbol_info_tick(resolved_symbol)
            bid = _first_positive_price(
                None if tick is None else getattr(tick, "bid", None),
                None if info is None else getattr(info, "bid", None),
            )
            ask = _first_positive_price(
                None if tick is None else getattr(tick, "ask", None),
                None if info is None else getattr(info, "ask", None),
            )
            midpoint = None if bid is None or ask is None else (bid + ask) / 2.0
            recent_bar_close, recent_bar_timestamp = self._recent_bar_snapshot(resolved_symbol)
            last = _first_positive_price(
                None if tick is None else getattr(tick, "last", None),
                None if info is None else getattr(info, "last", None),
                midpoint,
                recent_bar_close,
            )
            if last is not None:
                break
            if attempt + 1 < self.quote_poll_attempts and self.quote_poll_interval_seconds > 0.0:
                time.sleep(self.quote_poll_interval_seconds)
        else:
            raise RuntimeError(f"MT5 returned no usable price fields for '{resolved_symbol}'.")
        timestamp = self._resolve_timestamp(
            tick=tick,
            info=info,
            fallback=recent_bar_timestamp,
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

    def _ensure_symbol(self, symbol: str):
        mt5 = self._require_mt5()
        info = mt5.symbol_info(symbol)
        if info is None:
            raise ValueError(f"MT5 symbol '{symbol}' was not found.")
        if not info.visible:
            if not mt5.symbol_select(symbol, True):
                raise ValueError(f"MT5 symbol '{symbol}' could not be selected: {mt5.last_error()}")
            info = mt5.symbol_info(symbol)
            if info is None:
                raise ValueError(f"MT5 symbol '{symbol}' disappeared after selection.")
        return info

    def _recent_bar_snapshot(self, symbol: str) -> tuple[float | None, pd.Timestamp | None]:
        mt5 = self._require_mt5()
        for timeframe in (mt5.TIMEFRAME_M1, mt5.TIMEFRAME_D1):
            payload = mt5.copy_rates_from_pos(symbol, timeframe, 0, 1)
            if payload is None or len(payload) == 0:
                continue
            row = payload[-1]
            close = row["close"] if hasattr(row, "__getitem__") else getattr(row, "close", None)
            resolved_close = _first_positive_price(close)
            if resolved_close is None:
                continue
            raw_time = row["time"] if hasattr(row, "__getitem__") else getattr(row, "time", None)
            timestamp = None
            if raw_time not in {None, 0}:
                timestamp = pd.Timestamp(int(raw_time), unit="s", tz="UTC").tz_localize(None)
            return resolved_close, timestamp
        return None, None

    @staticmethod
    def _resolve_timestamp(
        *,
        tick: Any | None,
        info: Any | None,
        fallback: pd.Timestamp | None,
    ) -> pd.Timestamp:
        timestamp_value = None if tick is None else getattr(tick, "time_msc", None)
        if timestamp_value not in {None, 0}:
            return pd.Timestamp(int(timestamp_value), unit="ms", tz="UTC").tz_localize(None)
        timestamp_value = None if tick is None else getattr(tick, "time", None)
        if timestamp_value not in {None, 0}:
            return pd.Timestamp(int(timestamp_value), unit="s", tz="UTC").tz_localize(None)
        timestamp_value = None if info is None else getattr(info, "time", None)
        if timestamp_value not in {None, 0}:
            return pd.Timestamp(int(timestamp_value), unit="s", tz="UTC").tz_localize(None)
        if fallback is not None:
            return fallback
        return pd.Timestamp.utcnow().tz_localize(None)

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
