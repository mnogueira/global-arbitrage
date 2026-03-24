"""DuckDB-backed storage for opportunities and paper trades."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from global_arbitrage.core.models import StrategyObservation


class OpportunityStore:
    """Persist observations and trade events for later analysis."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def append_observation(self, observation: StrategyObservation) -> None:
        """Write one observation row into DuckDB."""

        record = observation.to_record()
        with duckdb.connect(str(self.path)) as conn:
            conn.execute(
                """
                INSERT INTO observations (
                    strategy_id,
                    strategy_name,
                    timestamp,
                    state,
                    signal,
                    gross_spread_bps,
                    net_edge_bps,
                    fair_value,
                    market_price,
                    total_cost_bps,
                    capital_required_brl,
                    notes_json,
                    metadata_json,
                    trade_legs_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    record["strategy_id"],
                    record["strategy_name"],
                    record["timestamp"],
                    record["state"],
                    record["signal"],
                    record["gross_spread_bps"],
                    record["net_edge_bps"],
                    record["fair_value"],
                    record["market_price"],
                    record["total_cost_bps"],
                    record["capital_required_brl"],
                    json.dumps(record["notes"]),
                    json.dumps(record["metadata"]),
                    json.dumps(record["trade_legs"]),
                ],
            )

    def append_trade_event(self, payload: dict[str, Any]) -> None:
        """Write one paper-trade event row into DuckDB."""

        with duckdb.connect(str(self.path)) as conn:
            conn.execute(
                """
                INSERT INTO trades (
                    strategy_id,
                    timestamp,
                    event_type,
                    side,
                    equity_brl,
                    pnl_brl,
                    hold_bars,
                    reason,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["strategy_id"],
                    payload["timestamp"],
                    payload["event_type"],
                    payload["side"],
                    payload["equity_brl"],
                    payload["pnl_brl"],
                    payload["hold_bars"],
                    payload["reason"],
                    json.dumps(payload.get("metadata", {})),
                ],
            )

    def recent_observations(self, limit: int = 25) -> pd.DataFrame:
        """Return the most recent observations."""

        with duckdb.connect(str(self.path)) as conn:
            return conn.execute(
                """
                SELECT *
                FROM observations
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()

    def recent_trades(self, limit: int = 25) -> pd.DataFrame:
        """Return the most recent trade events."""

        with duckdb.connect(str(self.path)) as conn:
            return conn.execute(
                """
                SELECT *
                FROM trades
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                [limit],
            ).fetchdf()

    def _ensure_schema(self) -> None:
        with duckdb.connect(str(self.path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS observations (
                    strategy_id VARCHAR,
                    strategy_name VARCHAR,
                    timestamp TIMESTAMP,
                    state VARCHAR,
                    signal INTEGER,
                    gross_spread_bps DOUBLE,
                    net_edge_bps DOUBLE,
                    fair_value DOUBLE,
                    market_price DOUBLE,
                    total_cost_bps DOUBLE,
                    capital_required_brl DOUBLE,
                    notes_json VARCHAR,
                    metadata_json VARCHAR,
                    trade_legs_json VARCHAR
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    strategy_id VARCHAR,
                    timestamp TIMESTAMP,
                    event_type VARCHAR,
                    side VARCHAR,
                    equity_brl DOUBLE,
                    pnl_brl DOUBLE,
                    hold_bars INTEGER,
                    reason VARCHAR,
                    metadata_json VARCHAR
                )
                """
            )
