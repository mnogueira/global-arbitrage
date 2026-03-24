import pytest

from global_arbitrage.core.models import TradeLeg
from global_arbitrage.execution.paper import calculate_unrealized_pnl


def test_calculate_unrealized_pnl_for_pair_trade() -> None:
    entry_legs = (
        TradeLeg("local", "local", 100.0, "BRL", direction=1, weight=1.0),
        TradeLeg("synthetic", "synthetic", 100.0, "BRL", direction=-1, weight=1.0),
    )
    current_legs = (
        TradeLeg("local", "local", 103.0, "BRL", direction=1, weight=1.0),
        TradeLeg("synthetic", "synthetic", 101.0, "BRL", direction=-1, weight=1.0),
    )
    pnl = calculate_unrealized_pnl(entry_legs, current_legs, gross_notional_brl=100000.0)
    assert round(pnl, 2) == 1000.0


def test_calculate_unrealized_pnl_rejects_currency_mismatch() -> None:
    entry_legs = (
        TradeLeg("local", "local", 100.0, "BRL", direction=1, weight=1.0),
        TradeLeg("synthetic", "synthetic", 100.0, "USD", direction=-1, weight=1.0),
    )
    current_legs = (
        TradeLeg("local", "local", 103.0, "BRL", direction=1, weight=1.0),
        TradeLeg("synthetic", "synthetic", 101.0, "USD", direction=-1, weight=1.0),
    )
    with pytest.raises(ValueError, match="share one currency"):
        calculate_unrealized_pnl(entry_legs, current_legs, gross_notional_brl=100000.0)
