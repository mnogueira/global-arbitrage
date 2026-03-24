"""Backtest one configured strategy."""

from __future__ import annotations

import argparse
from pathlib import Path

from rich.console import Console

from global_arbitrage.backtest.engine import BacktestEngine
from global_arbitrage.config.settings import load_yaml_config
from global_arbitrage.reporting.summary import build_backtest_table
from global_arbitrage.strategies import build_strategies


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest one strategy.")
    parser.add_argument("--config", default="configs/default.yaml")
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--period", default="2y")
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    console = Console()
    config = load_yaml_config(args.config)
    strategies = {strategy.strategy_id: strategy for strategy in build_strategies(config)}
    strategy = strategies.get(args.strategy)
    if strategy is None:
        raise SystemExit(f"Unknown strategy '{args.strategy}'.")

    paper_config = config.get("paper", {})
    engine = BacktestEngine(
        initial_equity_brl=float(paper_config.get("initial_equity_brl", 250000.0)),
        risk_fraction=float(paper_config.get("risk_fraction", 0.20)),
        max_drawdown=float(paper_config.get("max_drawdown", 0.15)),
        stop_loss_bps=float(paper_config.get("stop_loss_bps", 250.0)),
        take_profit_bps=float(paper_config.get("take_profit_bps", 300.0)),
    )
    observations = strategy.history(period=args.period, interval=args.interval)
    run = engine.run(observations)
    output_dir = args.output_dir or Path("storage") / "backtests" / args.strategy
    equity_path, trades_path = engine.save(run, output_dir=output_dir)
    console.print(build_backtest_table(run.summary))
    console.print(f"Saved equity curve to {equity_path}")
    console.print(f"Saved trades to {trades_path}")


if __name__ == "__main__":
    main()
