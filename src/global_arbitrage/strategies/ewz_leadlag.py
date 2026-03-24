"""EWZ to BOVA11 relative-value bridge."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import pandas as pd

from global_arbitrage.connectors.fx import BcbPtaxConnector
from global_arbitrage.connectors.yahoo import YahooFinanceConnector
from global_arbitrage.core.costs import CostAssumptions, classify_edge, signed_net_edge_bps
from global_arbitrage.core.models import StrategyObservation, TradeLeg
from global_arbitrage.core.utils import signal_from_edge, spread_bps_from_ratio
from global_arbitrage.strategies.base import ArbitrageStrategy


@dataclass(slots=True)
class EwzBovaBridgeStrategy(ArbitrageStrategy):
    """Translate EWZ into BRL and compare against BOVA11."""

    strategy_id: str
    local_symbol: str
    external_symbol: str
    lookback: int
    yahoo: YahooFinanceConnector
    fx: BcbPtaxConnector
    costs: CostAssumptions
    open_threshold_bps: float
    close_threshold_bps: float
    max_holding_bars: int
    capital_required_brl: float
    mt5_symbol: str | None = None
    proxy_symbol: str | None = None
    hedge_ratio_cache_ttl_seconds: int = 300
    _hedge_ratio_cache: float | None = field(default=None, init=False, repr=False)
    _hedge_ratio_cached_at: float | None = field(default=None, init=False, repr=False)

    @property
    def strategy_name(self) -> str:
        return f"EWZ bridge: {self.external_symbol} -> {self.local_symbol}"

    def refresh(self) -> StrategyObservation:
        local_quote = self.yahoo.latest_quote(self.local_symbol, currency="BRL")
        external_quote = self.yahoo.latest_quote(self.external_symbol, currency="USD")
        fx_quote = self.fx.latest_usdbrl()
        hedge_ratio = self._latest_hedge_ratio(use_cache=True)
        translated_external = external_quote.mid * fx_quote.mid
        fair_value = translated_external * hedge_ratio
        return self._build_observation(
            timestamp=max(local_quote.timestamp, external_quote.timestamp, fx_quote.timestamp),
            local_price=float(local_quote.mid),
            fair_value=float(fair_value),
            hedge_ratio=float(hedge_ratio),
            translated_external=float(translated_external),
            metadata={
                "local_symbol": self.local_symbol,
                "external_symbol": self.external_symbol,
                "translated_external_brl": translated_external,
                "hedge_ratio": hedge_ratio,
                "fx_usdbrl": fx_quote.mid,
                "proxy_symbol": self.proxy_symbol,
                "mt5_symbol": self.mt5_symbol,
            },
        )

    def history(self, *, period: str = "2y", interval: str = "1d") -> list[StrategyObservation]:
        local_history = self.yahoo.history(self.local_symbol, period=period, interval=interval)
        external_history = self.yahoo.history(self.external_symbol, period=period, interval=interval)
        fx_history = self.fx.history_usdbrl(period=period, interval=interval)
        min_periods = max(2, min(self.lookback, max(2, self.lookback // 2)))
        joined = pd.concat(
            [
                local_history["close"].rename("local"),
                external_history["close"].rename("external"),
                fx_history["close"].rename("fx"),
            ],
            axis=1,
        ).sort_index()
        joined = joined.ffill().dropna()
        joined["translated"] = joined["external"] * joined["fx"]
        joined["hedge_ratio"] = (
            (joined["local"] / joined["translated"])
            .rolling(self.lookback, min_periods=min_periods)
            .median()
            .shift(1)
        )
        joined = joined.dropna(subset=["hedge_ratio"])

        observations: list[StrategyObservation] = []
        for timestamp, row in joined.iterrows():
            fair_value = float(row["translated"] * row["hedge_ratio"])
            observations.append(
                self._build_observation(
                    timestamp=pd.Timestamp(timestamp).tz_localize(None),
                    local_price=float(row["local"]),
                    fair_value=fair_value,
                    hedge_ratio=float(row["hedge_ratio"]),
                    translated_external=float(row["translated"]),
                    metadata={
                        "local_symbol": self.local_symbol,
                        "external_symbol": self.external_symbol,
                        "translated_external_brl": float(row["translated"]),
                        "hedge_ratio": float(row["hedge_ratio"]),
                        "fx_usdbrl": float(row["fx"]),
                        "proxy_symbol": self.proxy_symbol,
                        "mt5_symbol": self.mt5_symbol,
                    },
                )
            )
        return observations

    def _latest_hedge_ratio(self, *, use_cache: bool = False) -> float:
        if use_cache and self._hedge_ratio_cache is not None and self._hedge_ratio_cached_at is not None:
            if time.monotonic() - self._hedge_ratio_cached_at <= self.hedge_ratio_cache_ttl_seconds:
                return self._hedge_ratio_cache
        local_history = self.yahoo.history(self.local_symbol, period="1y", interval="1d")
        external_history = self.yahoo.history(self.external_symbol, period="1y", interval="1d")
        fx_history = self.fx.history_usdbrl(period="1y", interval="1d")
        min_periods = max(2, min(self.lookback, max(2, self.lookback // 2)))
        joined = pd.concat(
            [
                local_history["close"].rename("local"),
                external_history["close"].rename("external"),
                fx_history["close"].rename("fx"),
            ],
            axis=1,
        ).sort_index()
        joined = joined.ffill().dropna()
        translated = joined["external"] * joined["fx"]
        ratio = (joined["local"] / translated).rolling(
            self.lookback,
            min_periods=min_periods,
        ).median()
        valid_ratio = ratio.dropna()
        if valid_ratio.empty:
            raise ValueError(
                "Unable to compute EWZ/BOVA hedge ratio because the rolling window produced no valid rows."
            )
        resolved = float(valid_ratio.iloc[-1])
        if use_cache:
            self._hedge_ratio_cache = resolved
            self._hedge_ratio_cached_at = time.monotonic()
        return resolved

    def _build_observation(
        self,
        *,
        timestamp: pd.Timestamp,
        local_price: float,
        fair_value: float,
        hedge_ratio: float,
        translated_external: float,
        metadata: dict[str, object],
    ) -> StrategyObservation:
        gross_spread_bps = spread_bps_from_ratio(fair_value / local_price)
        net_edge_bps = signed_net_edge_bps(gross_spread_bps, self.costs.total_bps)
        signal = signal_from_edge(net_edge_bps, self.close_threshold_bps)
        state = classify_edge(
            gross_spread_bps,
            net_edge_bps,
            open_threshold_bps=self.open_threshold_bps,
            close_threshold_bps=self.close_threshold_bps,
        )
        local_direction = 1 if gross_spread_bps >= 0.0 else -1
        trade_legs = (
            TradeLeg(
                instrument_id=f"b3:{self.local_symbol}",
                display_name=self.local_symbol,
                price=local_price,
                currency="BRL",
                direction=local_direction,
                weight=1.0,
                broker_symbol=self.mt5_symbol or self.proxy_symbol,
            ),
            TradeLeg(
                instrument_id=f"synthetic:{self.external_symbol}:translated",
                display_name=f"{self.external_symbol} translated with hedge ratio",
                price=fair_value,
                currency="BRL",
                direction=-local_direction,
                weight=1.0,
            ),
        )
        return StrategyObservation(
            strategy_id=self.strategy_id,
            strategy_name=self.strategy_name,
            timestamp=timestamp,
            state=state,
            signal=signal,
            gross_spread_bps=gross_spread_bps,
            net_edge_bps=net_edge_bps,
            fair_value=fair_value,
            market_price=local_price,
            total_cost_bps=self.costs.total_bps,
            capital_required_brl=self.capital_required_brl,
            trade_legs=trade_legs,
            open_threshold_bps=self.open_threshold_bps,
            close_threshold_bps=self.close_threshold_bps,
            max_holding_bars=self.max_holding_bars,
            notes=(
                "Positive spread means BOVA11 is cheap versus translated EWZ fair value",
                f"Rolling hedge ratio lookback: {self.lookback} bars",
            ),
            metadata={
                **metadata,
                "hedge_ratio": hedge_ratio,
                "translated_external_brl": translated_external,
            },
        )
