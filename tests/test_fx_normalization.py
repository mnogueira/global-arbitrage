import pandas as pd
import pytest

from global_arbitrage.connectors.fx import normalize_usdbrl_frame, normalize_usdbrl_series


def test_normalize_usdbrl_frame_keeps_direct_quotes() -> None:
    frame = pd.DataFrame(
        {
            "open": [5.1, 5.2],
            "high": [5.2, 5.3],
            "low": [5.0, 5.1],
            "close": [5.1, 5.2],
        }
    )
    normalized = normalize_usdbrl_frame(frame)
    assert normalized["close"].tolist() == [5.1, 5.2]


def test_normalize_usdbrl_frame_inverts_once_for_all_columns() -> None:
    frame = pd.DataFrame(
        {
            "open": [0.20, 0.19],
            "high": [0.21, 0.20],
            "low": [0.19, 0.18],
            "close": [0.20, 0.19],
        }
    )
    normalized = normalize_usdbrl_frame(frame)
    assert round(float(normalized["close"].iloc[0]), 4) == 5.0
    assert round(float(normalized["open"].iloc[1]), 4) == round(1.0 / 0.19, 4)


def test_normalize_usdbrl_series_rejects_unexpected_convention() -> None:
    with pytest.raises(ValueError, match="outside the expected"):
        normalize_usdbrl_series(pd.Series([1.1, 1.2, 1.3]))
