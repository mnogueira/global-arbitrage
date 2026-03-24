"""Crypto implied-FX arbitrage scanner."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from global_arbitrage.connectors.binance import BinanceSpotConnector
from global_arbitrage.connectors.bitso import BitsoConnector
from global_arbitrage.connectors.fx import BcbPtaxConnector
from global_arbitrage.connectors.yahoo import YahooFinanceConnector
from global_arbitrage.core.costs import CostAssumptions, classify_edge, signed_net_edge_bps
from global_arbitrage.core.models import StrategyObservation, TradeLeg
from global_arbitrage.core.utils import signal_from_edge, spread_bps_from_ratio
from global_arbitrage.strategies.base import ArbitrageStrategy


@dataclass(slots=True)
class CryptoImpliedFxStrategy(ArbitrageStrategy):
    """Compare local BTC/BRL with BTC/USD translated through USD/BRL."""

    strategy_id: str
    local_symbol: str
    usd_symbol: str
    yahoo: YahooFinanceConnector
    fx: BcbPtaxConnector
    binance: BinanceSpotConnector
    bitso: BitsoConnector
    costs: CostAssumptions
    open_threshold_bps: float
    close_threshold_bps: float
    max_holding_bars: int
    capital_required_brl: float
    bitso_books: tuple[str, ...] = ()
    mt5_symbol: str | None = None

    @property
    def strategy_name(self) -> str:
        return f"Crypto basis: {self.local_symbol} vs {self.usd_symbol}*USD/BRL"

    def refresh(self) -> StrategyObservation:
        local_quote = self.binance.latest_quote(self.local_symbol, currency="BRL")
        usd_quote = self.binance.latest_quote(self.usd_symbol, currency="USD")
        fx_quote = self.fx.latest_usdbrl()
        bitso_snapshots = {book: self.bitso.latest_quote(book).mid for book in self.bitso_books}
        fair_value = usd_quote.mid * fx_quote.mid
        return self._build_observation(
            timestamp=max(local_quote.timestamp, usd_quote.timestamp, fx_quote.timestamp),
            local_price=float(local_quote.mid),
            fair_value=float(fair_value),
            metadata={
                "local_symbol": self.local_symbol,
                "usd_symbol": self.usd_symbol,
                "local_quote_brl": local_quote.mid,
                "usd_quote": usd_quote.mid,
                "fx_usdbrl": fx_quote.mid,
                "bitso_quotes": bitso_snapshots,
                "mt5_symbol": self.mt5_symbol,
            },
        )

    def history(self, *, period: str = "2y", interval: str = "1d") -> list[StrategyObservation]:
        local_history = self.yahoo.history("BTC-BRL", period=period, interval=interval, currency="BRL")
        usd_history = self.yahoo.history("BTC-USD", period=period, interval=interval, currency="USD")
        fx_history = self.fx.history_usdbrl(period=period, interval=interval)
        joined = pd.concat(
            [
                local_history["close"].rename("local"),
                usd_history["close"].rename("usd"),
                fx_history["close"].rename("fx"),
            ],
            axis=1,
        ).sort_index()
        joined = joined.ffill().dropna()

        observations: list[StrategyObservation] = []
        for timestamp, row in joined.iterrows():
            fair_value = float(row["usd"] * row["fx"])
            observations.append(
                self._build_observation(
                    timestamp=pd.Timestamp(timestamp).tz_localize(None),
                    local_price=float(row["local"]),
                    fair_value=fair_value,
                    metadata={
                        "local_symbol": self.local_symbol,
                        "usd_symbol": self.usd_symbol,
                        "local_quote_brl": float(row["local"]),
                        "usd_quote": float(row["usd"]),
                        "fx_usdbrl": float(row["fx"]),
                        "bitso_quotes": {},
                        "mt5_symbol": self.mt5_symbol,
                    },
                )
            )
        return observations

    def _build_observation(
        self,
        *,
        timestamp: pd.Timestamp,
        local_price: float,
        fair_value: float,
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
                instrument_id=f"binance:{self.local_symbol}",
                display_name=self.local_symbol,
                price=local_price,
                currency="BRL",
                direction=local_direction,
                weight=1.0,
                broker_symbol=self.mt5_symbol,
                broker_venue="mt5" if self.mt5_symbol else None,
            ),
            TradeLeg(
                instrument_id=f"synthetic:{self.usd_symbol}:translated",
                display_name=f"{self.usd_symbol} translated to BRL",
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
                "Positive spread means BTC/BRL is cheap versus BTC/USD translated through USD/BRL",
                "LatAm venue snapshots are informational and not yet part of the pair P&L",
            ),
            metadata=metadata,
        )
