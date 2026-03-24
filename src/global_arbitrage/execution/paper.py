"""Local paper-trading engine for synthetic arbitrage positions."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from global_arbitrage.core.costs import half_turn_cost_brl
from global_arbitrage.core.models import StrategyObservation, TradeLeg
from global_arbitrage.core.store import OpportunityStore
from global_arbitrage.core.utils import assert_single_currency
from global_arbitrage.execution.mt5 import MT5ExecutionBroker, OrderSide


@dataclass(frozen=True, slots=True)
class PaperPosition:
    """Open paper position tied to one strategy."""

    signal: int
    opened_at: str
    equity_before_open_brl: float
    entry_equity_brl: float
    entry_notional_brl: float
    entry_cost_brl: float
    total_cost_bps: float
    bars_held: int
    entry_edge_bps: float
    trade_legs: tuple[TradeLeg, ...]

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["trade_legs"] = [asdict(leg) for leg in self.trade_legs]
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> PaperPosition:
        return cls(
            signal=int(payload["signal"]),
            opened_at=str(payload["opened_at"]),
            equity_before_open_brl=float(payload["equity_before_open_brl"]),
            entry_equity_brl=float(payload["entry_equity_brl"]),
            entry_notional_brl=float(payload["entry_notional_brl"]),
            entry_cost_brl=float(payload["entry_cost_brl"]),
            total_cost_bps=float(payload["total_cost_bps"]),
            bars_held=int(payload["bars_held"]),
            entry_edge_bps=float(payload["entry_edge_bps"]),
            trade_legs=tuple(TradeLeg(**leg) for leg in payload["trade_legs"]),
        )


@dataclass(frozen=True, slots=True)
class PaperState:
    """Persistent per-strategy state."""

    equity_brl: float
    peak_equity_brl: float
    wins: int
    losses: int
    open_position: PaperPosition | None = None

    @classmethod
    def initial(cls, initial_equity_brl: float) -> PaperState:
        return cls(
            equity_brl=initial_equity_brl,
            peak_equity_brl=initial_equity_brl,
            wins=0,
            losses=0,
            open_position=None,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "equity_brl": self.equity_brl,
            "peak_equity_brl": self.peak_equity_brl,
            "wins": self.wins,
            "losses": self.losses,
            "open_position": None if self.open_position is None else self.open_position.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> PaperState:
        open_position = payload.get("open_position")
        return cls(
            equity_brl=float(payload["equity_brl"]),
            peak_equity_brl=float(payload["peak_equity_brl"]),
            wins=int(payload["wins"]),
            losses=int(payload["losses"]),
            open_position=None if open_position is None else PaperPosition.from_dict(open_position),
        )


@dataclass(frozen=True, slots=True)
class PaperStepResult:
    """Outcome of one paper-execution step."""

    strategy_id: str
    action: str
    reason: str
    equity_brl: float
    unrealized_pnl_brl: float
    realized_pnl_brl: float | None
    state_path: str
    signals_path: str
    trades_path: str | None


def calculate_unrealized_pnl(entry_legs: tuple[TradeLeg, ...], current_legs: tuple[TradeLeg, ...], gross_notional_brl: float) -> float:
    """Mark a synthetic pair to market using current leg prices."""

    assert_single_currency(entry_legs, current_legs)
    current_lookup = {leg.instrument_id: leg for leg in current_legs}
    total_weight = sum(abs(leg.weight) for leg in entry_legs)
    if total_weight <= 0.0:
        raise ValueError("Trade legs must have positive absolute weight.")
    pnl = 0.0
    for entry_leg in entry_legs:
        current_leg = current_lookup.get(entry_leg.instrument_id)
        if current_leg is None:
            raise KeyError(f"Missing current price for leg '{entry_leg.instrument_id}'.")
        leg_notional = gross_notional_brl * abs(entry_leg.weight) / total_weight
        leg_return = (current_leg.price / entry_leg.price) - 1.0
        pnl += leg_notional * entry_leg.direction * leg_return
    return pnl


class PaperExecutionStore:
    """Filesystem-backed state and CSV journals."""

    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir)

    def state_path(self, strategy_id: str) -> Path:
        return self.root_dir / strategy_id / "state.json"

    def signals_path(self, strategy_id: str) -> Path:
        return self.root_dir / strategy_id / "signals.csv"

    def trades_path(self, strategy_id: str) -> Path:
        return self.root_dir / strategy_id / "trades.csv"

    def load_state(self, strategy_id: str, initial_equity_brl: float) -> PaperState:
        path = self.state_path(strategy_id)
        if not path.exists():
            return PaperState.initial(initial_equity_brl)
        return PaperState.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def save_state(self, strategy_id: str, state: PaperState) -> Path:
        path = self.state_path(strategy_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state.to_dict(), indent=2), encoding="utf-8")
        return path

    def append_signal(self, strategy_id: str, row: dict[str, object]) -> Path:
        return self._append_csv(self.signals_path(strategy_id), row)

    def append_trade(self, strategy_id: str, row: dict[str, object]) -> Path:
        return self._append_csv(self.trades_path(strategy_id), row)

    @staticmethod
    def _append_csv(path: Path, row: dict[str, object]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists()
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            if write_header:
                writer.writeheader()
            writer.writerow(row)
        return path


class PaperTrader:
    """Persistent paper trader using synthetic pair P&L."""

    def __init__(
        self,
        *,
        store: PaperExecutionStore,
        opportunity_store: OpportunityStore | None = None,
        risk_fraction: float = 0.20,
        max_drawdown: float = 0.15,
        stop_loss_bps: float = 250.0,
        take_profit_bps: float = 300.0,
        mt5_broker: MT5ExecutionBroker | None = None,
        mt5_order_quantity: float | None = None,
    ):
        self.store = store
        self.opportunity_store = opportunity_store
        self.risk_fraction = risk_fraction
        self.max_drawdown = max_drawdown
        self.stop_loss_bps = stop_loss_bps
        self.take_profit_bps = take_profit_bps
        self.mt5_broker = mt5_broker
        self.mt5_order_quantity = mt5_order_quantity

    def process_observation(self, observation: StrategyObservation, *, initial_equity_brl: float) -> PaperStepResult:
        state = self.store.load_state(observation.strategy_id, initial_equity_brl)
        position = state.open_position
        realized_pnl_brl: float | None = None
        trades_path: str | None = None
        action = "hold"
        reason = "no_action"
        unrealized_pnl_brl = 0.0

        if position is not None:
            unrealized_pnl_brl = calculate_unrealized_pnl(
                position.trade_legs,
                observation.trade_legs,
                position.entry_notional_brl,
            )
            marked_equity = position.entry_equity_brl + unrealized_pnl_brl
            peak_equity = max(state.peak_equity_brl, marked_equity)
            drawdown = 0.0 if peak_equity <= 0.0 else (peak_equity - marked_equity) / peak_equity
            trade_return_bps = 0.0 if position.entry_notional_brl <= 0.0 else unrealized_pnl_brl / position.entry_notional_brl * 10_000.0

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
                updated_position = PaperPosition(
                    signal=position.signal,
                    opened_at=position.opened_at,
                    equity_before_open_brl=position.equity_before_open_brl,
                    entry_equity_brl=position.entry_equity_brl,
                    entry_notional_brl=position.entry_notional_brl,
                    entry_cost_brl=position.entry_cost_brl,
                    total_cost_bps=position.total_cost_bps,
                    bars_held=position.bars_held + 1,
                    entry_edge_bps=position.entry_edge_bps,
                    trade_legs=position.trade_legs,
                )
                state = PaperState(
                    equity_brl=marked_equity,
                    peak_equity_brl=peak_equity,
                    wins=state.wins,
                    losses=state.losses,
                    open_position=updated_position,
                )
                reason = "marked_to_market"
            else:
                exit_cost_brl = half_turn_cost_brl(position.entry_notional_brl, position.total_cost_bps)
                exit_equity = marked_equity - exit_cost_brl
                realized_pnl_brl = exit_equity - position.equity_before_open_brl
                wins = state.wins + (1 if realized_pnl_brl > 0.0 else 0)
                losses = state.losses + (1 if realized_pnl_brl <= 0.0 else 0)
                state = PaperState(
                    equity_brl=exit_equity,
                    peak_equity_brl=max(peak_equity, exit_equity),
                    wins=wins,
                    losses=losses,
                    open_position=None,
                )
                action = "close"
                reason = exit_reason
                trades_path = str(
                    self.store.append_trade(
                        observation.strategy_id,
                        {
                            "timestamp": observation.timestamp.isoformat(),
                            "event_type": "close",
                            "side": "LONG" if position.signal > 0 else "SHORT",
                            "equity_brl": exit_equity,
                            "pnl_brl": realized_pnl_brl,
                            "hold_bars": position.bars_held,
                            "reason": exit_reason,
                            "net_edge_bps": observation.net_edge_bps,
                        },
                    )
                )
                self._mirror_primary_leg(observation, open_trade=False)
                if self.opportunity_store is not None:
                    self.opportunity_store.append_trade_event(
                        {
                            "strategy_id": observation.strategy_id,
                            "timestamp": observation.timestamp.isoformat(),
                            "event_type": "close",
                            "side": "LONG" if position.signal > 0 else "SHORT",
                            "equity_brl": exit_equity,
                            "pnl_brl": realized_pnl_brl,
                            "hold_bars": position.bars_held,
                            "reason": exit_reason,
                        }
                    )

        if state.open_position is None and observation.should_open:
            entry_notional_brl = state.equity_brl * self.risk_fraction
            entry_cost_brl = half_turn_cost_brl(entry_notional_brl, observation.total_cost_bps)
            entry_equity_brl = state.equity_brl - entry_cost_brl
            position = PaperPosition(
                signal=observation.signal.value,
                opened_at=observation.timestamp.isoformat(),
                equity_before_open_brl=state.equity_brl,
                entry_equity_brl=entry_equity_brl,
                entry_notional_brl=entry_notional_brl,
                entry_cost_brl=entry_cost_brl,
                total_cost_bps=observation.total_cost_bps,
                bars_held=0,
                entry_edge_bps=observation.net_edge_bps,
                trade_legs=observation.trade_legs,
            )
            state = PaperState(
                equity_brl=entry_equity_brl,
                peak_equity_brl=max(state.peak_equity_brl, entry_equity_brl),
                wins=state.wins,
                losses=state.losses,
                open_position=position,
            )
            action = "open" if action == "hold" else f"{action}_and_open"
            reason = "open_signal"
            trades_path = str(
                self.store.append_trade(
                    observation.strategy_id,
                    {
                        "timestamp": observation.timestamp.isoformat(),
                        "event_type": "open",
                        "side": "LONG" if observation.signal.value > 0 else "SHORT",
                        "equity_brl": entry_equity_brl,
                        "pnl_brl": -entry_cost_brl,
                        "hold_bars": 0,
                        "reason": "open_signal",
                        "net_edge_bps": observation.net_edge_bps,
                    },
                )
            )
            self._mirror_primary_leg(observation, open_trade=True)
            if self.opportunity_store is not None:
                self.opportunity_store.append_trade_event(
                    {
                        "strategy_id": observation.strategy_id,
                        "timestamp": observation.timestamp.isoformat(),
                        "event_type": "open",
                        "side": "LONG" if observation.signal.value > 0 else "SHORT",
                        "equity_brl": entry_equity_brl,
                        "pnl_brl": -entry_cost_brl,
                        "hold_bars": 0,
                        "reason": "open_signal",
                    }
                )

        signals_path = str(
            self.store.append_signal(
                observation.strategy_id,
                {
                    "timestamp": observation.timestamp.isoformat(),
                    "state": observation.state.value,
                    "signal": observation.signal.value,
                    "gross_spread_bps": observation.gross_spread_bps,
                    "net_edge_bps": observation.net_edge_bps,
                    "equity_brl": state.equity_brl,
                    "action": action,
                    "reason": reason,
                },
            )
        )
        state_path = str(self.store.save_state(observation.strategy_id, state))
        return PaperStepResult(
            strategy_id=observation.strategy_id,
            action=action,
            reason=reason,
            equity_brl=state.equity_brl,
            unrealized_pnl_brl=unrealized_pnl_brl,
            realized_pnl_brl=realized_pnl_brl,
            state_path=state_path,
            signals_path=signals_path,
            trades_path=trades_path,
        )

    def _mirror_primary_leg(self, observation: StrategyObservation, *, open_trade: bool) -> None:
        if self.mt5_broker is None or self.mt5_order_quantity is None or self.mt5_order_quantity <= 0.0:
            return
        primary_leg = next((leg for leg in observation.trade_legs if leg.broker_symbol), None)
        if primary_leg is None:
            return
        if open_trade:
            side = OrderSide.BUY if primary_leg.direction > 0 else OrderSide.SELL
        else:
            side = OrderSide.SELL if primary_leg.direction > 0 else OrderSide.BUY
        self.mt5_broker.submit_market_order(
            symbol=str(primary_leg.broker_symbol),
            side=side,
            quantity=self.mt5_order_quantity,
        )
