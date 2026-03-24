"""Simple historical simulator for strategy observations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from global_arbitrage.core.costs import half_turn_cost_brl
from global_arbitrage.core.models import BacktestSummary, StrategyObservation
from global_arbitrage.execution.paper import calculate_unrealized_pnl


@dataclass(frozen=True, slots=True)
class BacktestRun:
    """Backtest outputs."""

    summary: BacktestSummary
    equity_curve: pd.DataFrame
    trades: pd.DataFrame


@dataclass(slots=True)
class _SimPosition:
    signal: int
    opened_at: pd.Timestamp
    equity_before_open_brl: float
    entry_equity_brl: float
    entry_notional_brl: float
    total_cost_bps: float
    bars_held: int
    trade_legs: tuple


class BacktestEngine:
    """Run a simple event-style backtest over historical observations."""

    def __init__(
        self,
        *,
        initial_equity_brl: float = 100000.0,
        risk_fraction: float = 0.20,
        max_drawdown: float = 0.15,
        stop_loss_bps: float = 250.0,
        take_profit_bps: float = 300.0,
    ):
        self.initial_equity_brl = initial_equity_brl
        self.risk_fraction = risk_fraction
        self.max_drawdown = max_drawdown
        self.stop_loss_bps = stop_loss_bps
        self.take_profit_bps = take_profit_bps

    def run(self, observations: list[StrategyObservation]) -> BacktestRun:
        if not observations:
            raise ValueError("Backtest requires at least one observation.")

        equity = self.initial_equity_brl
        peak = equity
        position: _SimPosition | None = None
        curve_rows: list[dict[str, object]] = []
        trade_rows: list[dict[str, object]] = []

        for observation in observations:
            marked_equity = equity
            unrealized_pnl = 0.0

            if position is not None:
                unrealized_pnl = calculate_unrealized_pnl(
                    position.trade_legs,
                    observation.trade_legs,
                    position.entry_notional_brl,
                )
                marked_equity = position.entry_equity_brl + unrealized_pnl
                peak = max(peak, marked_equity)
                drawdown = 0.0 if peak <= 0.0 else (peak - marked_equity) / peak
                trade_return_bps = 0.0 if position.entry_notional_brl <= 0.0 else unrealized_pnl / position.entry_notional_brl * 10_000.0
                exit_reason: str | None = None
                if drawdown >= self.max_drawdown:
                    exit_reason = "drawdown_limit"
                elif trade_return_bps <= -self.stop_loss_bps:
                    exit_reason = "stop_loss"
                elif trade_return_bps >= self.take_profit_bps:
                    exit_reason = "take_profit"
                elif position.bars_held >= observation.max_holding_bars:
                    exit_reason = "max_holding"
                elif observation.signal.value == 0:
                    exit_reason = "flat_signal"
                elif observation.signal.value != position.signal:
                    exit_reason = "signal_flip"
                elif abs(observation.gross_spread_bps) <= observation.close_threshold_bps:
                    exit_reason = "spread_closed"

                if exit_reason is None:
                    position.bars_held += 1
                else:
                    exit_cost_brl = half_turn_cost_brl(position.entry_notional_brl, position.total_cost_bps)
                    exit_equity = marked_equity - exit_cost_brl
                    realized_pnl = exit_equity - position.equity_before_open_brl
                    equity = exit_equity
                    peak = max(peak, equity)
                    trade_rows.append(
                        {
                            "opened_at": position.opened_at.isoformat(),
                            "closed_at": observation.timestamp.isoformat(),
                            "side": "LONG" if position.signal > 0 else "SHORT",
                            "pnl_brl": realized_pnl,
                            "hold_bars": position.bars_held,
                            "reason": exit_reason,
                            "ending_equity_brl": equity,
                        }
                    )
                    position = None
                    marked_equity = equity

            if position is None and observation.should_open:
                entry_notional_brl = equity * self.risk_fraction
                entry_cost_brl = half_turn_cost_brl(entry_notional_brl, observation.total_cost_bps)
                entry_equity_brl = equity - entry_cost_brl
                position = _SimPosition(
                    signal=observation.signal.value,
                    opened_at=observation.timestamp,
                    equity_before_open_brl=equity,
                    entry_equity_brl=entry_equity_brl,
                    entry_notional_brl=entry_notional_brl,
                    total_cost_bps=observation.total_cost_bps,
                    bars_held=0,
                    trade_legs=observation.trade_legs,
                )
                equity = entry_equity_brl
                marked_equity = equity
                peak = max(peak, equity)
                trade_rows.append(
                    {
                        "opened_at": observation.timestamp.isoformat(),
                        "closed_at": None,
                        "side": "LONG" if observation.signal.value > 0 else "SHORT",
                        "pnl_brl": -entry_cost_brl,
                        "hold_bars": 0,
                        "reason": "open_signal",
                        "ending_equity_brl": equity,
                    }
                )

            curve_rows.append(
                {
                    "timestamp": observation.timestamp.isoformat(),
                    "equity_brl": marked_equity,
                    "gross_spread_bps": observation.gross_spread_bps,
                    "net_edge_bps": observation.net_edge_bps,
                    "signal": observation.signal.value,
                    "position_open": position is not None,
                }
            )

        equity_curve = pd.DataFrame(curve_rows)
        trades = pd.DataFrame(trade_rows)
        closed_trades = trades[trades["closed_at"].notna()].copy() if not trades.empty else pd.DataFrame()
        wins = int((closed_trades["pnl_brl"] > 0.0).sum()) if not closed_trades.empty else 0
        closed_count = int(len(closed_trades))
        avg_trade_pnl = float(closed_trades["pnl_brl"].mean()) if not closed_trades.empty else 0.0
        if equity_curve.empty:
            max_drawdown_pct = 0.0
            ending_equity = self.initial_equity_brl
        else:
            rolling_peak = equity_curve["equity_brl"].cummax()
            drawdowns = (rolling_peak - equity_curve["equity_brl"]) / rolling_peak.replace(0.0, pd.NA)
            max_drawdown_pct = float(drawdowns.fillna(0.0).max() * 100.0)
            ending_equity = float(equity_curve["equity_brl"].iloc[-1])

        summary = BacktestSummary(
            strategy_id=observations[0].strategy_id,
            trades=closed_count,
            wins=wins,
            ending_equity_brl=ending_equity,
            total_return_pct=((ending_equity / self.initial_equity_brl) - 1.0) * 100.0,
            max_drawdown_pct=max_drawdown_pct,
            avg_trade_pnl_brl=avg_trade_pnl,
            win_rate_pct=0.0 if closed_count == 0 else wins / closed_count * 100.0,
        )
        return BacktestRun(summary=summary, equity_curve=equity_curve, trades=trades)

    @staticmethod
    def save(run: BacktestRun, *, output_dir: str | Path) -> tuple[Path, Path]:
        """Save equity curve and trades to CSV files."""

        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        equity_path = output / "equity_curve.csv"
        trades_path = output / "trades.csv"
        run.equity_curve.to_csv(equity_path, index=False)
        run.trades.to_csv(trades_path, index=False)
        return equity_path, trades_path
