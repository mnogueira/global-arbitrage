"""Small shared helpers used across strategies and execution."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd

from global_arbitrage.core.models import SignalSide, TradeLeg


def to_naive_timestamp(value: pd.Timestamp) -> pd.Timestamp:
    """Normalize timestamps to naive UTC-style pandas timestamps."""

    timestamp = pd.Timestamp(value)
    return timestamp.tz_convert(None) if timestamp.tz is not None else timestamp


def spread_bps_from_ratio(ratio: float) -> float:
    """Convert a price ratio into basis points."""

    return (ratio - 1.0) * 10_000.0


def signal_from_edge(edge_bps: float, close_threshold_bps: float) -> SignalSide:
    """Translate a signed edge into a trading direction."""

    if abs(edge_bps) < close_threshold_bps:
        return SignalSide.FLAT
    return SignalSide.LONG if edge_bps > 0.0 else SignalSide.SHORT


def assert_single_currency(entry_legs: tuple[TradeLeg, ...], current_legs: tuple[TradeLeg, ...]) -> str:
    """Ensure a synthetic pair is being marked in one reference currency."""

    entry_currencies = {leg.currency for leg in entry_legs}
    current_currencies = {leg.currency for leg in current_legs}
    if len(entry_currencies) != 1:
        raise ValueError(f"Entry legs must share one currency. Got {sorted(entry_currencies)}")
    if len(current_currencies) != 1:
        raise ValueError(f"Current legs must share one currency. Got {sorted(current_currencies)}")
    entry_currency = next(iter(entry_currencies))
    current_currency = next(iter(current_currencies))
    if entry_currency != current_currency:
        raise ValueError(
            "Entry and current legs must use the same reference currency. "
            f"Got entry={entry_currency}, current={current_currency}"
        )
    return entry_currency


def assert_timestamp_fresh(
    timestamp: pd.Timestamp,
    *,
    max_age: timedelta,
    reference_now: pd.Timestamp | None = None,
) -> None:
    """Raise if a quote timestamp is older than the allowed freshness window."""

    normalized = to_naive_timestamp(timestamp)
    now = to_naive_timestamp(pd.Timestamp.utcnow()) if reference_now is None else to_naive_timestamp(reference_now)
    age = now - normalized
    if age > pd.Timedelta(max_age):
        raise ValueError(
            "Quote timestamp is stale. "
            f"timestamp={normalized.isoformat()} age={age} max_age={max_age}"
        )
