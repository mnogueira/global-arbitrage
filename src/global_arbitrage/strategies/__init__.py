"""Strategy registry and builders."""

from __future__ import annotations

from typing import Any

from global_arbitrage.config.settings import Settings
from global_arbitrage.connectors.binance import BinanceSpotConnector
from global_arbitrage.connectors.bitso import BitsoConnector
from global_arbitrage.connectors.fx import BcbPtaxConnector
from global_arbitrage.connectors.ib import IBContractSpec, InteractiveBrokersConnector
from global_arbitrage.connectors.mt5 import MT5Connector
from global_arbitrage.connectors.yahoo import YahooFinanceConnector
from global_arbitrage.core.costs import CostAssumptions
from global_arbitrage.strategies.adr_parity import ADRParityStrategy
from global_arbitrage.strategies.base import ArbitrageStrategy
from global_arbitrage.strategies.bridge import HedgeRatioBridgeStrategy
from global_arbitrage.strategies.crypto_implied_fx import CryptoImpliedFxStrategy
from global_arbitrage.strategies.ewz_leadlag import EwzBovaBridgeStrategy


def build_strategies(config: dict[str, Any]) -> list[ArbitrageStrategy]:
    """Instantiate strategy objects from the YAML config."""

    settings = Settings()
    strategy_block = config.get("strategies", {})
    ib_config = config.get("brokers", {}).get("ib", {})
    mt5_config = config.get("brokers", {}).get("mt5", {})
    ib_client_id = int(ib_config.get("client_id", settings.ib_client_id))
    ib_data_port = int(ib_config.get("data_port", ib_config.get("port", settings.ib_data_port)))
    ib_execution_port = int(
        ib_config.get("execution_port", ib_config.get("port", settings.ib_execution_port))
    )
    raw_ib_data_client_id = ib_config.get("data_client_id", settings.ib_data_client_id)
    ib_data_client_id = None if raw_ib_data_client_id is None else int(raw_ib_data_client_id)
    raw_ib_execution_client_id = ib_config.get(
        "execution_client_id",
        settings.ib_execution_client_id,
    )
    ib_execution_client_id = (
        None if raw_ib_execution_client_id is None else int(raw_ib_execution_client_id)
    )
    mt5 = MT5Connector(
        login=settings.mt5_login,
        password=settings.mt5_password,
        server=settings.mt5_server,
        mt5_path=settings.mt5_path,
        symbol_aliases=_build_mt5_symbol_aliases(strategy_block),
    )
    ib = InteractiveBrokersConnector(
        host=str(ib_config.get("host", settings.ib_host)),
        port=ib_data_port,
        data_port=ib_data_port,
        execution_port=ib_execution_port,
        client_id=ib_client_id,
        data_client_id=ib_data_client_id,
        execution_client_id=ib_execution_client_id,
        timeout=float(ib_config.get("timeout_seconds", settings.ib_timeout_seconds)),
        readonly=bool(ib_config.get("readonly", settings.ib_readonly)),
        account=ib_config.get("account") or settings.ib_account,
        base_currency=str(ib_config.get("base_currency", settings.ib_base_currency)),
        market_data_type=int(ib_config.get("market_data_type", settings.ib_market_data_type)),
        contract_overrides=_build_ib_contract_overrides(config),
    )
    yahoo = YahooFinanceConnector()
    fx = BcbPtaxConnector(
        yahoo=yahoo,
        market=mt5,
        market_symbol=str(mt5_config.get("fx_proxy_symbol", "WDO$N")),
        market_scale=float(mt5_config.get("fx_proxy_scale", 1000.0)),
        prefer_market_proxy=bool(mt5_config.get("prefer_fx_proxy", True)),
    )
    binance = BinanceSpotConnector()
    bitso = BitsoConnector()
    strategies: list[ArbitrageStrategy] = []
    notional_brl = float(config.get("scanner", {}).get("default_notional_brl", 100000.0))

    for payload in strategy_block.get("adr_parity", []):
        strategies.append(
            ADRParityStrategy(
                strategy_id=str(payload["id"]),
                local_symbol=str(payload["local_symbol"]),
                adr_symbol=str(payload["adr_symbol"]),
                local_name=str(payload["local_name"]),
                shares_per_adr=float(payload["shares_per_adr"]),
                local_market=mt5,
                adr_market=ib,
                fx=fx,
                costs=CostAssumptions.from_dict(payload.get("costs")),
                open_threshold_bps=float(payload["open_threshold_bps"]),
                close_threshold_bps=float(payload["close_threshold_bps"]),
                max_holding_bars=int(payload["max_holding_bars"]),
                capital_required_brl=notional_brl,
                mt5_symbol=payload.get("mt5_symbol"),
                ib_symbol=payload.get("ib_symbol"),
                local_market_symbol=payload.get("local_market_symbol"),
                adr_market_symbol=payload.get("adr_market_symbol"),
            )
        )

    for payload in strategy_block.get("ewz_bova", []):
        strategies.append(
            EwzBovaBridgeStrategy(
                strategy_id=str(payload["id"]),
                local_symbol=str(payload["local_symbol"]),
                external_symbol=str(payload["external_symbol"]),
                lookback=int(payload["lookback"]),
                local_market=mt5,
                external_market=ib,
                fx=fx,
                costs=CostAssumptions.from_dict(payload.get("costs")),
                open_threshold_bps=float(payload["open_threshold_bps"]),
                close_threshold_bps=float(payload["close_threshold_bps"]),
                max_holding_bars=int(payload["max_holding_bars"]),
                capital_required_brl=notional_brl,
                local_name=payload.get("local_name"),
                external_name=payload.get("external_name"),
                strategy_label=payload.get("label"),
                local_market_symbol=payload.get("local_market_symbol"),
                external_market_symbol=payload.get("external_market_symbol"),
                external_currency=str(payload.get("external_currency", "USD")),
                mt5_symbol=payload.get("mt5_symbol"),
                ib_symbol=payload.get("ib_symbol"),
                proxy_symbol=payload.get("proxy_symbol"),
                local_order_quantity_multiplier=float(
                    payload.get("local_order_quantity_multiplier", 1.0)
                ),
            )
        )

    for payload in strategy_block.get("cross_market", []):
        strategies.append(
            HedgeRatioBridgeStrategy(
                strategy_id=str(payload["id"]),
                local_symbol=str(payload["local_symbol"]),
                external_symbol=str(payload["external_symbol"]),
                lookback=int(payload["lookback"]),
                local_market=mt5,
                external_market=ib,
                fx=fx,
                costs=CostAssumptions.from_dict(payload.get("costs")),
                open_threshold_bps=float(payload["open_threshold_bps"]),
                close_threshold_bps=float(payload["close_threshold_bps"]),
                max_holding_bars=int(payload["max_holding_bars"]),
                capital_required_brl=notional_brl,
                local_name=payload.get("local_name"),
                external_name=payload.get("external_name"),
                strategy_label=payload.get("label"),
                local_market_symbol=payload.get("local_market_symbol"),
                external_market_symbol=payload.get("external_market_symbol"),
                external_currency=str(payload.get("external_currency", "USD")),
                mt5_symbol=payload.get("mt5_symbol"),
                ib_symbol=payload.get("ib_symbol"),
                proxy_symbol=payload.get("proxy_symbol"),
                local_order_quantity_multiplier=float(
                    payload.get("local_order_quantity_multiplier", 1.0)
                ),
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


def _build_ib_contract_overrides(config: dict[str, Any]) -> dict[str, IBContractSpec]:
    raw_contracts = config.get("brokers", {}).get("ib", {}).get("contracts", {})
    if not isinstance(raw_contracts, dict):
        raise ValueError("brokers.ib.contracts must be a mapping of symbol aliases to IB contract specs.")
    return {
        str(alias): IBContractSpec.from_dict(payload)
        for alias, payload in raw_contracts.items()
        if isinstance(payload, dict)
    }


def _build_mt5_symbol_aliases(strategy_block: dict[str, Any]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for payload in strategy_block.get("adr_parity", []):
        local_symbol = payload.get("local_symbol")
        mt5_symbol = payload.get("mt5_symbol")
        if local_symbol and mt5_symbol:
            aliases[str(local_symbol)] = str(mt5_symbol)
    for payload in strategy_block.get("ewz_bova", []):
        local_symbol = payload.get("local_symbol")
        mt5_symbol = payload.get("mt5_symbol") or payload.get("proxy_symbol")
        if local_symbol and mt5_symbol:
            aliases[str(local_symbol)] = str(mt5_symbol)
    for payload in strategy_block.get("cross_market", []):
        local_symbol = payload.get("local_symbol")
        mt5_symbol = payload.get("mt5_symbol") or payload.get("proxy_symbol")
        if local_symbol and mt5_symbol:
            aliases[str(local_symbol)] = str(mt5_symbol)
    return aliases
