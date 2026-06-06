from __future__ import annotations

import pandas as pd

from tw_quant.risk.controls import RiskManager, detect_price_jumps


def test_detect_price_jumps_returns_empty_for_normal_sequence() -> None:
    prices = pd.DataFrame(
        [
            {"trade_date": "2026-06-01", "symbol": "2330", "close": 100.0},
            {"trade_date": "2026-06-02", "symbol": "2330", "close": 103.0},
            {"trade_date": "2026-06-03", "symbol": "2330", "close": 101.0},
        ]
    )

    assert detect_price_jumps(prices).empty


def test_detect_price_jumps_marks_0050_like_wrong_price_with_tighter_threshold() -> None:
    prices = pd.DataFrame(
        [
            {"trade_date": "2026-05-29", "symbol": "0050", "close": 105.40},
            {"trade_date": "2026-05-30", "symbol": "0050", "close": 81.20},
            {"trade_date": "2026-06-01", "symbol": "0050", "close": 105.50},
        ]
    )

    jumps = detect_price_jumps(prices, max_abs_daily_return=0.20)

    assert len(jumps) == 2
    assert list(jumps.columns) == ["symbol", "trade_date", "close", "prev_close", "return_1d", "reason"]
    assert set(jumps["symbol"]) == {"0050"}
    assert "abs(return_1d)" in jumps.iloc[0]["reason"]


def test_validate_price_data_can_block_extreme_daily_jump() -> None:
    prices = pd.DataFrame(
        [
            {"trade_date": "2026-06-01", "symbol": "2330", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000},
            {"trade_date": "2026-06-02", "symbol": "2330", "open": 40, "high": 41, "low": 39, "close": 40, "volume": 1000},
        ]
    )

    try:
        RiskManager().validate_price_data(prices, check_price_jumps=True)
    except Exception as exc:
        assert "extreme daily jumps" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected price jump validation to fail")
