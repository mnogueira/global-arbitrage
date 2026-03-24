"""Advance the paper-trading state using fresh scanner observations."""

from __future__ import annotations

import argparse
import time

from rich.console import Console

from global_arbitrage.config.settings import Settings, load_yaml_config
from global_arbitrage.core.scanner import ArbitrageScanner
from global_arbitrage.core.store import OpportunityStore
from global_arbitrage.execution.ib import IBExecutionBroker
from global_arbitrage.execution.mt5 import MT5ExecutionBroker
from global_arbitrage.execution.paper import PaperExecutionStore, PaperTrader
from global_arbitrage.execution.router import BrokerRouter
from global_arbitrage.reporting.summary import build_observations_table
from global_arbitrage.strategies import build_strategies


def main() -> None:
    parser = argparse.ArgumentParser(description="Advance the paper book with live observations.")
    parser.add_argument("--config", default="configs/default.yaml")
    parser.add_argument("--strategy", action="append", default=[])
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=30.0)
    parser.add_argument("--mirror-to-mt5", action="store_true")
    parser.add_argument("--mirror-to-ib", action="store_true")
    parser.add_argument("--mt5-order-quantity", type=float, default=None)
    parser.add_argument("--ib-order-quantity", type=float, default=None)
    args = parser.parse_args()

    console = Console()
    config = load_yaml_config(args.config)
    settings = Settings()
    ib_config = config.get("brokers", {}).get("ib", {})
    opportunity_store = OpportunityStore(config["store"]["path"])
    scanner = ArbitrageScanner(
        strategies=build_strategies(config),
        store=opportunity_store,
        alert_threshold_bps=float(config.get("scanner", {}).get("alert_threshold_bps", 35.0)),
    )

    brokers: dict[str, object] = {}
    default_order_quantities: dict[str, float] = {}
    if args.mirror_to_mt5:
        brokers["mt5"] = MT5ExecutionBroker(
            login=settings.mt5_login,
            password=settings.mt5_password,
            server=settings.mt5_server,
            mt5_path=settings.mt5_path,
            magic_number=settings.mt5_magic_number,
            deviation=settings.mt5_deviation,
        )
        if args.mt5_order_quantity is not None:
            default_order_quantities["mt5"] = args.mt5_order_quantity
    if args.mirror_to_ib:
        brokers["ib"] = IBExecutionBroker(
            host=str(ib_config.get("host", settings.ib_host)),
            port=int(ib_config.get("port", settings.ib_port)),
            client_id=int(ib_config.get("client_id", settings.ib_client_id)),
            timeout=float(ib_config.get("timeout_seconds", settings.ib_timeout_seconds)),
            readonly=bool(ib_config.get("readonly", settings.ib_readonly)),
            account=ib_config.get("account") or settings.ib_account,
            base_currency=str(ib_config.get("base_currency", settings.ib_base_currency)),
            market_data_type=int(ib_config.get("market_data_type", settings.ib_market_data_type)),
        )
        if args.ib_order_quantity is not None:
            default_order_quantities["ib"] = args.ib_order_quantity

    broker_router = None
    if brokers:
        broker_router = BrokerRouter(
            brokers=brokers,
            default_order_quantities=default_order_quantities,
        )
        broker_router.connect_all()

    paper_config = config.get("paper", {})
    trader = PaperTrader(
        store=PaperExecutionStore(config["store"]["alerts_root"]),
        opportunity_store=opportunity_store,
        risk_fraction=float(paper_config.get("risk_fraction", 0.20)),
        max_drawdown=float(paper_config.get("max_drawdown", 0.15)),
        stop_loss_bps=float(paper_config.get("stop_loss_bps", 250.0)),
        take_profit_bps=float(paper_config.get("take_profit_bps", 300.0)),
        broker_router=broker_router,
    )
    initial_equity = float(paper_config.get("initial_equity_brl", 250000.0))
    strategy_ids = set(args.strategy) if args.strategy else None

    try:
        for index in range(args.iterations):
            observations = scanner.run_once(strategy_ids=strategy_ids)
            console.print(build_observations_table(observations))
            for observation in observations:
                result = trader.process_observation(observation, initial_equity_brl=initial_equity)
                broker_suffix = ""
                if result.combined_broker_snapshot is not None:
                    snapshot = result.combined_broker_snapshot
                    if snapshot.equity_brl is not None:
                        broker_suffix = f" broker_equity_brl={snapshot.equity_brl:,.2f}"
                console.print(
                    f"{result.strategy_id}: action={result.action} reason={result.reason} "
                    f"equity={result.equity_brl:,.2f}{broker_suffix}"
                )
            if index + 1 < args.iterations:
                time.sleep(args.sleep_seconds)
    finally:
        if broker_router is not None:
            broker_router.disconnect_all()


if __name__ == "__main__":
    main()
