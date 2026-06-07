from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.reporting.risk_adjusted_alpha import risk_adjusted_alpha_snapshot


def test_risk_adjusted_alpha_marks_insufficient_samples(tmp_path: Path) -> None:
    _write_equity(tmp_path, _series(100, [0.01, -0.005]))
    _write_market_indices(tmp_path, _series(100, [0.004, 0.003]))

    result = risk_adjusted_alpha_snapshot(tmp_path, "2026-12-31")

    assert result["conclusion_status"] == "DATA_INSUFFICIENT"
    assert result["risk_adjusted_alpha_status"] == "DATA_INSUFFICIENT"
    assert result["excess_return_5d"] is None


def test_short_sample_positive_alpha_is_short_term_only(tmp_path: Path) -> None:
    _write_equity(tmp_path, _series(100, [0.003] * 21))
    _write_market_indices(tmp_path, _series(100, [0.001] * 21))
    _write_trades(tmp_path, count=20)

    result = risk_adjusted_alpha_snapshot(tmp_path, "2026-12-31")

    assert result["primary_alpha_window"] == "20d"
    assert result["excess_return_20d"] > 0
    assert result["risk_adjusted_alpha_status"] == "OUTPERFORMING_SHORT_TERM"
    assert result["conclusion_status"] == "OUTPERFORMING_SHORT_TERM"


def test_positive_return_with_high_risk_is_not_confirmed(tmp_path: Path) -> None:
    returns = [0.01] * 10 + [-0.25] + [0.015] * 50
    _write_equity(tmp_path, _series(100, returns))
    _write_market_indices(tmp_path, _series(100, [0.001] * 61))
    _write_trades(tmp_path, count=60)

    result = risk_adjusted_alpha_snapshot(tmp_path, "2026-12-31")

    assert result["primary_alpha_window"] == "60d"
    assert result["excess_return_60d"] > 0
    assert result["conclusion_status"] != "OUTPERFORMING_CONFIRMED"
    assert result["risk_adjusted_alpha_status"] == "RISK_TOO_HIGH"


def _series(start: float, returns: list[float]) -> list[float]:
    values = [start]
    current = start
    for value in returns:
        current *= 1 + value
        values.append(round(current, 6))
    return values


def _write_equity(path: Path, values: list[float]) -> None:
    dates = pd.bdate_range("2026-06-01", periods=len(values))
    pd.DataFrame(
        [
            {
                "trade_date": day.strftime("%Y-%m-%d"),
                "total_equity_after_cost": value,
                "total_return_pct": value / values[0] - 1.0,
            }
            for day, value in zip(dates, values)
        ]
    ).to_csv(path / f"pnl_chart_data_{dates[-1].strftime('%Y%m%d')}.csv", index=False, encoding="utf-8-sig")


def _write_market_indices(path: Path, values: list[float]) -> None:
    dates = pd.bdate_range("2026-06-01", periods=len(values))
    pd.DataFrame(
        [
            {
                "trade_date": day.strftime("%Y-%m-%d"),
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": value,
                "high": value,
                "low": value,
                "close": value,
                "source": "test",
                "is_official": True,
            }
            for day, value in zip(dates, values)
        ]
    ).to_csv(path / "market_indices.csv", index=False, encoding="utf-8-sig")


def _write_trades(path: Path, count: int) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-01",
                "stock_id": f"23{index:02d}",
                "entry_price": 100 + index,
                "shares": 100,
                "status": "CLOSED",
            }
            for index in range(count)
        ]
    ).to_csv(path / "paper_trades.csv", index=False, encoding="utf-8-sig")
