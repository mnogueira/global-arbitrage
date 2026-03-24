"""Main scanner orchestration."""

from __future__ import annotations

from collections.abc import Iterable

from global_arbitrage.core.alerts import AlertSink, ConsoleAlertSink
from global_arbitrage.core.models import StrategyObservation
from global_arbitrage.core.store import OpportunityStore
from global_arbitrage.strategies.base import ArbitrageStrategy


class ArbitrageScanner:
    """Run strategy instances and persist their observations."""

    def __init__(
        self,
        *,
        strategies: Iterable[ArbitrageStrategy],
        store: OpportunityStore,
        alert_threshold_bps: float = 35.0,
        alert_sink: AlertSink | None = None,
    ):
        self.strategies = list(strategies)
        self.store = store
        self.alert_threshold_bps = alert_threshold_bps
        self.alert_sink = alert_sink or ConsoleAlertSink()

    def run_once(self, *, strategy_ids: set[str] | None = None) -> list[StrategyObservation]:
        """Scan all configured strategies once."""

        observations: list[StrategyObservation] = []
        for strategy in self.strategies:
            if strategy_ids and strategy.strategy_id not in strategy_ids:
                continue
            observation = strategy.refresh()
            self.store.append_observation(observation)
            if observation.signal.value != 0 and observation.abs_net_edge_bps >= self.alert_threshold_bps:
                self.alert_sink.send(observation)
            observations.append(observation)
        return observations
