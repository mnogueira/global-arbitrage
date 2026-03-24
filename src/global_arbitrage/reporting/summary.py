"""Console rendering helpers."""

from __future__ import annotations

from rich.table import Table

from global_arbitrage.core.models import BacktestSummary, StrategyObservation


def build_observations_table(observations: list[StrategyObservation]) -> Table:
    """Render scanner observations as a Rich table."""

    table = Table(title="Arbitrage Scanner")
    table.add_column("Strategy")
    table.add_column("State")
    table.add_column("Signal")
    table.add_column("Gross (bps)", justify="right")
    table.add_column("Net (bps)", justify="right")
    table.add_column("Costs (bps)", justify="right")
    table.add_column("Market", justify="right")
    table.add_column("Fair", justify="right")
    for observation in observations:
        signal = "LONG" if observation.signal.value > 0 else "SHORT" if observation.signal.value < 0 else "FLAT"
        table.add_row(
            observation.strategy_id,
            observation.state.value,
            signal,
            f"{observation.gross_spread_bps:.1f}",
            f"{observation.net_edge_bps:.1f}",
            f"{observation.total_cost_bps:.1f}",
            f"{observation.market_price:.4f}",
            f"{observation.fair_value:.4f}",
        )
    return table


def build_backtest_table(summary: BacktestSummary) -> Table:
    """Render a compact backtest summary."""

    table = Table(title=f"Backtest Summary: {summary.strategy_id}")
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Trades", str(summary.trades))
    table.add_row("Wins", str(summary.wins))
    table.add_row("Ending Equity (BRL)", f"{summary.ending_equity_brl:,.2f}")
    table.add_row("Total Return", f"{summary.total_return_pct:.2f}%")
    table.add_row("Max Drawdown", f"{summary.max_drawdown_pct:.2f}%")
    table.add_row("Average Trade P&L", f"{summary.avg_trade_pnl_brl:,.2f}")
    table.add_row("Win Rate", f"{summary.win_rate_pct:.2f}%")
    return table
