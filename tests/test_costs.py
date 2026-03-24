from global_arbitrage.core.costs import CostAssumptions, half_turn_cost_brl, signed_net_edge_bps


def test_cost_assumptions_total_bps() -> None:
    costs = CostAssumptions(
        exchange_fee_bps=5.0,
        fx_bps=10.0,
        slippage_bps=7.0,
        borrow_bps=3.0,
    )
    assert costs.total_bps == 25.0


def test_signed_net_edge_preserves_direction() -> None:
    assert signed_net_edge_bps(120.0, 40.0) == 80.0
    assert signed_net_edge_bps(-120.0, 40.0) == -80.0
    assert signed_net_edge_bps(20.0, 40.0) == 0.0


def test_half_turn_cost_sums_to_round_trip() -> None:
    round_trip = 80.0
    notional = 100000.0
    assert half_turn_cost_brl(notional, round_trip) * 2 == notional * (round_trip / 10_000.0)
