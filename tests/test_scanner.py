import pandas as pd

from global_arbitrage.core.models import OpportunityState, SignalSide, StrategyObservation, TradeLeg
from global_arbitrage.core.scanner import ArbitrageScanner


class FakeStore:
    def __init__(self) -> None:
        self.observations: list[StrategyObservation] = []

    def append_observation(self, observation: StrategyObservation) -> None:
        self.observations.append(observation)


class PassingStrategy:
    strategy_id = "ok_strategy"
    strategy_name = "Passing strategy"

    def refresh(self) -> StrategyObservation:
        return StrategyObservation(
            strategy_id=self.strategy_id,
            strategy_name=self.strategy_name,
            timestamp=pd.Timestamp("2026-03-24 15:00:00"),
            state=OpportunityState.WATCH,
            signal=SignalSide.FLAT,
            gross_spread_bps=10.0,
            net_edge_bps=5.0,
            fair_value=100.0,
            market_price=99.0,
            total_cost_bps=5.0,
            capital_required_brl=100000.0,
            trade_legs=(
                TradeLeg(
                    instrument_id="local:test",
                    display_name="Test",
                    price=99.0,
                    currency="BRL",
                    direction=1,
                ),
            ),
            open_threshold_bps=25.0,
            close_threshold_bps=5.0,
            max_holding_bars=3,
        )


class FailingStrategy:
    strategy_id = "bad_strategy"
    strategy_name = "Failing strategy"

    def refresh(self) -> StrategyObservation:
        raise RuntimeError("boom")


def test_scanner_continues_after_one_strategy_fails() -> None:
    store = FakeStore()
    scanner = ArbitrageScanner(strategies=[PassingStrategy(), FailingStrategy()], store=store)

    observations = scanner.run_once()

    assert [observation.strategy_id for observation in observations] == ["ok_strategy"]
    assert [observation.strategy_id for observation in store.observations] == ["ok_strategy"]
    assert len(scanner.last_errors) == 1
    assert scanner.last_errors[0].strategy_id == "bad_strategy"
    assert scanner.last_errors[0].error_type == "RuntimeError"
