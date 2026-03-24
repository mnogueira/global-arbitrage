"""Cost assumptions and edge accounting."""

from __future__ import annotations

from dataclasses import dataclass

from global_arbitrage.core.models import OpportunityState


@dataclass(frozen=True, slots=True)
class CostAssumptions:
    """Explicit round-trip cost assumptions in basis points."""

    exchange_fee_bps: float = 0.0
    fx_bps: float = 0.0
    slippage_bps: float = 0.0
    borrow_bps: float = 0.0
    settlement_bps: float = 0.0
    tracking_bps: float = 0.0
    transfer_bps: float = 0.0
    financing_bps: float = 0.0
    extra_buffer_bps: float = 0.0

    @classmethod
    def from_dict(cls, payload: dict[str, float] | None) -> CostAssumptions:
        """Build a cost object from a config mapping."""

        if payload is None:
            return cls()
        return cls(
            exchange_fee_bps=float(payload.get("exchange_fee_bps", 0.0)),
            fx_bps=float(payload.get("fx_bps", 0.0)),
            slippage_bps=float(payload.get("slippage_bps", 0.0)),
            borrow_bps=float(payload.get("borrow_bps", 0.0)),
            settlement_bps=float(payload.get("settlement_bps", 0.0)),
            tracking_bps=float(payload.get("tracking_bps", 0.0)),
            transfer_bps=float(payload.get("transfer_bps", 0.0)),
            financing_bps=float(payload.get("financing_bps", 0.0)),
            extra_buffer_bps=float(payload.get("extra_buffer_bps", 0.0)),
        )

    @property
    def total_bps(self) -> float:
        """Total round-trip cost assumption.

        This value is defined as entry plus exit. Execution code should split it
        into two half-turn charges, one at open and one at close.
        """

        return (
            self.exchange_fee_bps
            + self.fx_bps
            + self.slippage_bps
            + self.borrow_bps
            + self.settlement_bps
            + self.tracking_bps
            + self.transfer_bps
            + self.financing_bps
            + self.extra_buffer_bps
        )


def signed_net_edge_bps(gross_spread_bps: float, total_cost_bps: float) -> float:
    """Subtract costs from a signed spread while preserving direction."""

    sign = 1.0 if gross_spread_bps >= 0.0 else -1.0
    residual = max(abs(gross_spread_bps) - total_cost_bps, 0.0)
    return sign * residual


def half_turn_cost_brl(notional_brl: float, roundtrip_cost_bps: float) -> float:
    """Apply exactly half of a round-trip cost assumption."""

    if roundtrip_cost_bps < 0.0:
        raise ValueError("Round-trip costs must be non-negative.")
    return notional_brl * (roundtrip_cost_bps / 10_000.0) / 2.0


def classify_edge(
    gross_spread_bps: float,
    net_edge_bps: float,
    *,
    open_threshold_bps: float,
    close_threshold_bps: float,
) -> OpportunityState:
    """Map a spread snapshot to a simple opportunity state."""

    if abs(net_edge_bps) >= open_threshold_bps:
        return OpportunityState.OPEN
    if abs(gross_spread_bps) >= close_threshold_bps:
        return OpportunityState.WATCH
    return OpportunityState.PASS_
