"""Generic cross-market bridge strategy with a rolling hedge ratio."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import pandas as pd

from global_arbitrage.connectors.base import MarketDataConnector
from global_arbitrage.connectors.fx import BcbPtaxConnector
from global_arbitrage.core.costs import CostAssumptions, classify_edge, signed_net_edge_bps
from global_arbitrage.core.models import StrategyObservation, TradeLeg
from global_arbitrage.core.utils import signal_from_edge, spread_bps_from_ratio
from global_arbitrage.strategies.base import ArbitrageStrategy


@dataclass(slots=True)
class HedgeRatioBridgeStrategy(ArbitrageStrategy):
    """Translate an external instrument into BRL and compare it to a local market leg."""

    strategy_id: str
    local_symbol: str
    external_symbol: str
    lookback: int
    local_market: MarketDataConnector
    external_market: MarketDataConnector
    fx: BcbPtaxConnector
    costs: CostAssumptions
    open_threshold_bps: float
    close_threshold_bps: float
    max_holding_bars: int
    capital_required_brl: float
    strategy_label: str | None = None
    local_name: str | None = None
    external_name: str | None = None
    external_currency: str = "USD"
    local_market_symbol: str | None = None
    external_market_symbol: str | None = None
    mt5_symbol: str | None = None
    ib_symbol: str | None = None
    proxy_symbol: str | None = None
    local_order_quantity_multiplier: float = 1.0
    hedge_ratio_cache_ttl_seconds: int = 300
    _hedge_ratio_cache: float | None = field(default=None, init=False, repr=False)
    _hedge_ratio_cached_at: float | None = field(default=None, init=False, repr=False)

    @property
    def strategy_name(self) -> str:
        return self.strategy_label or f"Bridge: {self.external_symbol} -> {self.local_symbol}"

    def refresh(self) -> StrategyObservation:
        local_quote = self.local_market.latest_quote(self._local_quote_symbol(), currency="BRL")
        external_quote = self.external_market.latest_quote(
            self._external_quote_symbol(),
            currency=self.external_currency,
        )
        fx_quote = self.fx.latest_usdbrl() if self.external_currency != "BRL" else None
        hedge_ratio = self._latest_hedge_ratio(use_cache=True)
        fx_rate = 1.0 if fx_quote is None else fx_quote.mid
        translated_external = self._translate_external_price(external_quote.mid, fx_rate)
        fair_value = translated_external * hedge_ratio
        return self._build_observation(
            timestamp=max(
                local_quote.timestamp,
                external_quote.timestamp,
                local_quote.timestamp if fx_quote is None else fx_quote.timestamp,
            ),
            local_price=float(local_quote.mid),
            fair_value=float(fair_value),
            hedge_ratio=float(hedge_ratio),
            translated_external=float(translated_external),
            metadata={
                "local_symbol": self.local_symbol,
                "external_symbol": self.external_symbol,
                "translated_external_brl": translated_external,
                "hedge_ratio": hedge_ratio,
                "fx_usdbrl": fx_rate,
                "proxy_symbol": self.proxy_symbol,
                "mt5_symbol": self.mt5_symbol,
                "ib_symbol": self.ib_symbol,
                "local_venue": local_quote.venue,
                "external_venue": external_quote.venue,
                "local_market_symbol": self._local_quote_symbol(),
                "external_market_symbol": self._external_quote_symbol(),
                "external_currency": self.external_currency,
            },
        )

    def history(self, *, period: str = "2y", interval: str = "1d") -> list[StrategyObservation]:
        local_history = self.local_market.history(self._local_quote_symbol(), period=period, interval=interval)
        external_history = self.external_market.history(
            self._external_quote_symbol(),
            period=period,
            interval=interval,
        )
        fx_history = self._fx_history(period=period, interval=interval)
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
        joined["translated"] = joined.apply(
            lambda row: self._translate_external_price(float(row["external"]), float(row["fx"])),
            axis=1,
        )
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
                        "ib_symbol": self.ib_symbol,
                        "local_market_symbol": self._local_quote_symbol(),
                        "external_market_symbol": self._external_quote_symbol(),
                        "external_currency": self.external_currency,
                    },
                )
            )
        return observations

    def _latest_hedge_ratio(self, *, use_cache: bool = False) -> float:
        if use_cache and self._hedge_ratio_cache is not None and self._hedge_ratio_cached_at is not None:
            if time.monotonic() - self._hedge_ratio_cached_at <= self.hedge_ratio_cache_ttl_seconds:
                return self._hedge_ratio_cache
        local_history = self.local_market.history(self._local_quote_symbol(), period="1y", interval="1d")
        external_history = self.external_market.history(
            self._external_quote_symbol(),
            period="1y",
            interval="1d",
        )
        fx_history = self._fx_history(period="1y", interval="1d")
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
        translated = joined.apply(
            lambda row: self._translate_external_price(float(row["external"]), float(row["fx"])),
            axis=1,
        )
        ratio = (joined["local"] / translated).rolling(
            self.lookback,
            min_periods=min_periods,
        ).median()
        valid_ratio = ratio.dropna()
        if valid_ratio.empty:
            raise ValueError(
                "Unable to compute the bridge hedge ratio because the rolling window produced no valid rows."
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
        local_display = self.local_name or self.local_symbol
        external_display = self.external_name or self.external_symbol
        trade_legs = (
            TradeLeg(
                instrument_id=f"local:{self.local_symbol}",
                display_name=local_display,
                price=local_price,
                currency="BRL",
                direction=local_direction,
                weight=1.0,
                broker_symbol=self.mt5_symbol or self.proxy_symbol or self._local_quote_symbol(),
                broker_venue="mt5" if (self.mt5_symbol or self.proxy_symbol or self.local_market_symbol) else None,
                order_quantity_multiplier=self.local_order_quantity_multiplier,
            ),
            TradeLeg(
                instrument_id=f"synthetic:{self.external_symbol}:translated",
                display_name=f"{external_display} translated with hedge ratio",
                price=fair_value,
                currency="BRL",
                direction=-local_direction,
                weight=1.0,
                broker_symbol=self.ib_symbol,
                broker_venue="ib" if self.ib_symbol else None,
                order_quantity_multiplier=abs(hedge_ratio),
                metadata={"raw_symbol": self._external_quote_symbol()},
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
                "Positive spread means the local leg is cheap versus translated external fair value",
                f"Rolling hedge ratio lookback: {self.lookback} bars",
            ),
            metadata={
                **metadata,
                "hedge_ratio": hedge_ratio,
                "translated_external_brl": translated_external,
            },
        )

    def _translate_external_price(self, external_price: float, fx_usdbrl: float) -> float:
        if self.external_currency == "BRL":
            return external_price
        if self.external_currency == "USD":
            return external_price * fx_usdbrl
        raise ValueError(f"Unsupported bridge external currency '{self.external_currency}'.")

    def _local_quote_symbol(self) -> str:
        return self.local_market_symbol or self.mt5_symbol or self.proxy_symbol or self.local_symbol

    def _external_quote_symbol(self) -> str:
        return self.external_market_symbol or self.ib_symbol or self.external_symbol

    def _fx_history(self, *, period: str, interval: str) -> pd.DataFrame:
        if self.external_currency == "BRL":
            local_history = self.local_market.history(self._local_quote_symbol(), period=period, interval=interval)
            return pd.DataFrame({"close": 1.0}, index=local_history.index)
        return self.fx.history_usdbrl(period=period, interval=interval)
