"""ADR parity scanner for Brazilian blue chips."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from global_arbitrage.connectors.base import MarketDataConnector
from global_arbitrage.connectors.fx import BcbPtaxConnector
from global_arbitrage.core.costs import CostAssumptions, classify_edge, signed_net_edge_bps
from global_arbitrage.core.models import StrategyObservation, TradeLeg
from global_arbitrage.core.utils import signal_from_edge, spread_bps_from_ratio
from global_arbitrage.strategies.base import ArbitrageStrategy


@dataclass(slots=True)
class ADRParityStrategy(ArbitrageStrategy):
    """Compare local Brazilian shares with translated ADR fair value."""

    strategy_id: str
    local_symbol: str
    adr_symbol: str
    local_name: str
    shares_per_adr: float
    local_market: MarketDataConnector
    adr_market: MarketDataConnector
    fx: BcbPtaxConnector
    costs: CostAssumptions
    open_threshold_bps: float
    close_threshold_bps: float
    max_holding_bars: int
    capital_required_brl: float
    mt5_symbol: str | None = None
    ib_symbol: str | None = None
    local_market_symbol: str | None = None
    adr_market_symbol: str | None = None

    @property
    def strategy_name(self) -> str:
        return f"ADR parity: {self.local_symbol} vs {self.adr_symbol}"

    def refresh(self) -> StrategyObservation:
        local_quote = self.local_market.latest_quote(self._resolved_local_market_symbol, currency="BRL")
        adr_quote = self.adr_market.latest_quote(self._resolved_adr_market_symbol, currency="USD")
        fx_quote = self.fx.latest_usdbrl()
        implied_local = adr_quote.mid * fx_quote.mid / self.shares_per_adr
        return self._build_observation(
            timestamp=max(local_quote.timestamp, adr_quote.timestamp, fx_quote.timestamp),
            local_price=float(local_quote.mid),
            implied_local=float(implied_local),
            metadata={
                "local_symbol": self.local_symbol,
                "adr_symbol": self.adr_symbol,
                "shares_per_adr": self.shares_per_adr,
                "fx_usdbrl": fx_quote.mid,
                "local_quote_brl": local_quote.mid,
                "adr_quote_usd": adr_quote.mid,
                "local_market_symbol": self._resolved_local_market_symbol,
                "adr_market_symbol": self._resolved_adr_market_symbol,
                "mt5_symbol": self.mt5_symbol,
                "ib_symbol": self.ib_symbol,
            },
        )

    def history(self, *, period: str = "2y", interval: str = "1d") -> list[StrategyObservation]:
        local_history = self.local_market.history(
            self._resolved_local_market_symbol,
            period=period,
            interval=interval,
            currency="BRL",
        )
        adr_history = self.adr_market.history(
            self._resolved_adr_market_symbol,
            period=period,
            interval=interval,
            currency="USD",
        )
        fx_history = self.fx.history_usdbrl(period=period, interval=interval)
        joined = pd.concat(
            [
                local_history["close"].rename("local"),
                adr_history["close"].rename("adr"),
                fx_history["close"].rename("fx"),
            ],
            axis=1,
        ).sort_index()
        joined = joined.ffill().dropna()

        observations: list[StrategyObservation] = []
        for timestamp, row in joined.iterrows():
            implied_local = float(row["adr"] * row["fx"] / self.shares_per_adr)
            observations.append(
                self._build_observation(
                    timestamp=pd.Timestamp(timestamp).tz_localize(None),
                    local_price=float(row["local"]),
                    implied_local=implied_local,
                    metadata={
                        "local_symbol": self.local_symbol,
                        "adr_symbol": self.adr_symbol,
                        "shares_per_adr": self.shares_per_adr,
                        "fx_usdbrl": float(row["fx"]),
                        "local_quote_brl": float(row["local"]),
                        "adr_quote_usd": float(row["adr"]),
                        "local_market_symbol": self._resolved_local_market_symbol,
                        "adr_market_symbol": self._resolved_adr_market_symbol,
                        "mt5_symbol": self.mt5_symbol,
                        "ib_symbol": self.ib_symbol,
                    },
                )
            )
        return observations

    @property
    def _resolved_local_market_symbol(self) -> str:
        return self.local_market_symbol or self.mt5_symbol or self.local_symbol

    @property
    def _resolved_adr_market_symbol(self) -> str:
        return self.adr_market_symbol or self.ib_symbol or self.adr_symbol

    def _build_observation(
        self,
        *,
        timestamp: pd.Timestamp,
        local_price: float,
        implied_local: float,
        metadata: dict[str, object],
    ) -> StrategyObservation:
        gross_spread_bps = spread_bps_from_ratio(implied_local / local_price)
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
                display_name=self.local_name,
                price=local_price,
                currency="BRL",
                direction=local_direction,
                weight=1.0,
                broker_symbol=self.mt5_symbol,
                broker_venue="mt5" if self.mt5_symbol else None,
                order_quantity_multiplier=self.shares_per_adr,
            ),
            TradeLeg(
                instrument_id=f"synthetic:{self.adr_symbol}:translated",
                display_name=f"{self.adr_symbol} translated to BRL/share",
                price=implied_local,
                currency="BRL",
                direction=-local_direction,
                weight=1.0,
                broker_symbol=self.ib_symbol,
                broker_venue="ib" if self.ib_symbol else None,
                order_quantity_multiplier=1.0,
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
            fair_value=implied_local,
            market_price=local_price,
            total_cost_bps=self.costs.total_bps,
            capital_required_brl=self.capital_required_brl,
            trade_legs=trade_legs,
            open_threshold_bps=self.open_threshold_bps,
            close_threshold_bps=self.close_threshold_bps,
            max_holding_bars=self.max_holding_bars,
            notes=(
                f"1 ADR = {self.shares_per_adr:g} local shares",
                "Positive spread means local shares are cheap versus ADR parity",
            ),
            metadata=metadata,
        )
