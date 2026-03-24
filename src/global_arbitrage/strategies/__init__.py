"""Strategy registry and builders."""

from __future__ import annotations

from typing import Any

from global_arbitrage.connectors.binance import BinanceSpotConnector
from global_arbitrage.connectors.bitso import BitsoConnector
from global_arbitrage.connectors.fx import BcbPtaxConnector
from global_arbitrage.connectors.yahoo import YahooFinanceConnector
from global_arbitrage.core.costs import CostAssumptions
from global_arbitrage.strategies.adr_parity import ADRParityStrategy
from global_arbitrage.strategies.base import ArbitrageStrategy
from global_arbitrage.strategies.crypto_implied_fx import CryptoImpliedFxStrategy
from global_arbitrage.strategies.ewz_leadlag import EwzBovaBridgeStrategy


def build_strategies(config: dict[str, Any]) -> list[ArbitrageStrategy]:
    """Instantiate strategy objects from the YAML config."""

    yahoo = YahooFinanceConnector()
    fx = BcbPtaxConnector(yahoo=yahoo)
    binance = BinanceSpotConnector()
    bitso = BitsoConnector()
    strategies: list[ArbitrageStrategy] = []
    strategy_block = config.get("strategies", {})
    notional_brl = float(config.get("scanner", {}).get("default_notional_brl", 100000.0))

    for payload in strategy_block.get("adr_parity", []):
        strategies.append(
            ADRParityStrategy(
                strategy_id=str(payload["id"]),
                local_symbol=str(payload["local_symbol"]),
                adr_symbol=str(payload["adr_symbol"]),
                local_name=str(payload["local_name"]),
                shares_per_adr=float(payload["shares_per_adr"]),
                yahoo=yahoo,
                fx=fx,
                costs=CostAssumptions.from_dict(payload.get("costs")),
                open_threshold_bps=float(payload["open_threshold_bps"]),
                close_threshold_bps=float(payload["close_threshold_bps"]),
                max_holding_bars=int(payload["max_holding_bars"]),
                capital_required_brl=notional_brl,
                mt5_symbol=payload.get("mt5_symbol"),
            )
        )

    for payload in strategy_block.get("ewz_bova", []):
        strategies.append(
            EwzBovaBridgeStrategy(
                strategy_id=str(payload["id"]),
                local_symbol=str(payload["local_symbol"]),
                external_symbol=str(payload["external_symbol"]),
                lookback=int(payload["lookback"]),
                yahoo=yahoo,
                fx=fx,
                costs=CostAssumptions.from_dict(payload.get("costs")),
                open_threshold_bps=float(payload["open_threshold_bps"]),
                close_threshold_bps=float(payload["close_threshold_bps"]),
                max_holding_bars=int(payload["max_holding_bars"]),
                capital_required_brl=notional_brl,
                mt5_symbol=payload.get("mt5_symbol"),
                proxy_symbol=payload.get("proxy_symbol"),
            )
        )

    for payload in strategy_block.get("crypto_implied_fx", []):
        strategies.append(
            CryptoImpliedFxStrategy(
                strategy_id=str(payload["id"]),
                local_symbol=str(payload["local_symbol"]),
                usd_symbol=str(payload["usd_symbol"]),
                yahoo=yahoo,
                fx=fx,
                binance=binance,
                bitso=bitso,
                costs=CostAssumptions.from_dict(payload.get("costs")),
                open_threshold_bps=float(payload["open_threshold_bps"]),
                close_threshold_bps=float(payload["close_threshold_bps"]),
                max_holding_bars=int(payload["max_holding_bars"]),
                capital_required_brl=notional_brl,
                bitso_books=tuple(payload.get("bitso_books", [])),
                mt5_symbol=payload.get("mt5_symbol"),
            )
        )

    return strategies
