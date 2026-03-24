"""Run the arbitrage scanner."""

from __future__ import annotations

import argparse
import time

from rich.console import Console

from global_arbitrage.config.settings import load_yaml_config
from global_arbitrage.core.scanner import ArbitrageScanner
from global_arbitrage.core.store import OpportunityStore
from global_arbitrage.reporting.summary import build_observations_table
from global_arbitrage.strategies import build_strategies


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the global arbitrage scanner.")
    parser.add_argument("--config", default="configs/default.yaml")
    parser.add_argument("--strategy", action="append", default=[])
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=30.0)
    args = parser.parse_args()

    console = Console()
    config = load_yaml_config(args.config)
    store = OpportunityStore(config["store"]["path"])
    scanner = ArbitrageScanner(
        strategies=build_strategies(config),
        store=store,
        alert_threshold_bps=float(config.get("scanner", {}).get("alert_threshold_bps", 35.0)),
    )
    strategy_ids = set(args.strategy) if args.strategy else None
    for index in range(args.iterations):
        observations = scanner.run_once(strategy_ids=strategy_ids)
        console.print(build_observations_table(observations))
        for error in scanner.last_errors:
            console.print(
                f"[yellow]{error.strategy_id} skipped: {error.error_type}: {error.message}[/yellow]"
            )
        if index + 1 < args.iterations:
            time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    main()
