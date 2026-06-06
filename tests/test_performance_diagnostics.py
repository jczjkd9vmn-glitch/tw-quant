from __future__ import annotations

from pathlib import Path

import pandas as pd

from tw_quant.reporting.performance_diagnostics import (
    PERFORMANCE_DIAGNOSTICS_COLUMNS,
    generate_performance_diagnostics,
)


def test_performance_diagnostics_no_equity_data_does_not_crash(tmp_path: Path) -> None:
    result = generate_performance_diagnostics(tmp_path, trade_date="2026-06-05")

    assert list(result.frame.columns) == PERFORMANCE_DIAGNOSTICS_COLUMNS
    assert result.frame.iloc[0]["status"] == "DATA_INSUFFICIENT"
    assert "no pnl_chart_data" in result.frame.iloc[0]["data_quality_warning"]
    assert (tmp_path / "performance_diagnostics_20260605.csv").exists()


def test_performance_diagnostics_calculates_metrics_from_pnl_chart(tmp_path: Path) -> None:
    _write_pnl_chart(tmp_path)
    _write_market_regime(tmp_path)

    result = generate_performance_diagnostics(tmp_path, trade_date="2026-06-05")
    row = result.frame.iloc[0]

    assert row["source"] == "pnl_chart_data"
    assert row["observation_count"] == 6
    assert row["daily_return_count"] == 5
    assert row["cumulative_return"] == 0.06
    assert row["max_drawdown"] < 0
    assert row["sharpe_like_ratio"] != ""
    assert row["benchmark_source"] == "加權指數"
    assert row["benchmark_return"] == 0.02
    assert row["alpha"] == 0.04
    assert row["status"] == "OK"


def test_performance_diagnostics_marks_small_samples_insufficient(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {"trade_date": "2026-06-04", "total_equity_after_cost": 1_000_000, "total_return_pct": 0.0},
            {"trade_date": "2026-06-05", "total_equity_after_cost": 1_010_000, "total_return_pct": 0.01},
        ]
    ).to_csv(tmp_path / "pnl_chart_data_20260605.csv", index=False, encoding="utf-8-sig")
    _write_market_regime(tmp_path)

    result = generate_performance_diagnostics(tmp_path, trade_date="2026-06-05")
    row = result.frame.iloc[0]

    assert row["status"] == "DATA_INSUFFICIENT"
    assert "daily_return_count < 5" in row["data_quality_warning"]


def test_performance_diagnostics_falls_back_to_paper_summary(tmp_path: Path) -> None:
    for day, equity in [
        ("2026-06-01", 1_000_000),
        ("2026-06-02", 1_020_000),
        ("2026-06-03", 1_010_000),
        ("2026-06-04", 1_030_000),
        ("2026-06-05", 1_025_000),
        ("2026-06-06", 1_040_000),
    ]:
        pd.DataFrame(
            [
                {
                    "trade_date": day,
                    "total_capital": 1_000_000,
                    "total_equity_after_cost": equity,
                }
            ]
        ).to_csv(tmp_path / f"paper_summary_{day.replace('-', '')}.csv", index=False, encoding="utf-8-sig")
    _write_sector_strength(tmp_path)

    result = generate_performance_diagnostics(tmp_path, trade_date="2026-06-06")
    row = result.frame.iloc[0]

    assert row["source"] == "paper_summary"
    assert row["cumulative_return"] == 0.04
    assert row["benchmark_source"] == "0050 fallback"
    assert "fallback 使用 0050" in row["benchmark_warning"]


def _write_pnl_chart(path: Path) -> None:
    pd.DataFrame(
        [
            {"trade_date": "2026-05-30", "total_equity_after_cost": 1_000_000, "total_return_pct": 0.00},
            {"trade_date": "2026-06-01", "total_equity_after_cost": 1_020_000, "total_return_pct": 0.02},
            {"trade_date": "2026-06-02", "total_equity_after_cost": 1_015_000, "total_return_pct": 0.015},
            {"trade_date": "2026-06-03", "total_equity_after_cost": 1_035_000, "total_return_pct": 0.035},
            {"trade_date": "2026-06-04", "total_equity_after_cost": 1_030_000, "total_return_pct": 0.03},
            {"trade_date": "2026-06-05", "total_equity_after_cost": 1_060_000, "total_return_pct": 0.06},
        ]
    ).to_csv(path / "pnl_chart_data_20260605.csv", index=False, encoding="utf-8-sig")


def _write_market_regime(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "source": "index",
                "market_return_5d": 2.0,
                "market_return_20d": 5.0,
            }
        ]
    ).to_csv(path / "market_regime_20260605.csv", index=False, encoding="utf-8-sig")


def _write_sector_strength(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-06",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.02,
                "stock_return_20d": 0.04,
                "market_return_5d": 0.01,
                "market_return_20d": 0.03,
            }
        ]
    ).to_csv(path / "sector_strength.csv", index=False, encoding="utf-8-sig")
