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
    _write_market_indices(tmp_path)
    _write_valid_trades(tmp_path, count=20)
    _write_portfolio(tmp_path, count=3)

    result = generate_performance_diagnostics(tmp_path, trade_date="2026-06-05")
    row = result.frame.iloc[0]

    assert row["source"] == "pnl_chart_data"
    assert row["observation_count"] == 6
    assert row["daily_return_count"] == 5
    assert row["cumulative_return"] == 0.06
    assert row["max_drawdown"] < 0
    assert row["sharpe_like_ratio"] != ""
    assert row["benchmark_source"] == "正式加權報酬指數"
    assert row["benchmark_return"] == 0.02
    assert row["alpha"] == 0.04
    assert row["strategy_history_days"] == 6
    assert row["valid_trade_count"] == 20
    assert row["holding_record_count"] == 3
    assert bool(row["can_judge_strategy_alpha"]) is True
    assert bool(row["can_judge_strategy_alpha_5d"]) is True
    assert bool(row["can_judge_strategy_alpha_20d"]) is False
    assert row["conclusion_status"] == "OK"
    assert row["status"] == "OK_WITH_WARNINGS"


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


def test_performance_diagnostics_requires_strategy_sample_for_alpha(tmp_path: Path) -> None:
    _write_pnl_chart(tmp_path)
    _write_market_indices(tmp_path)

    result = generate_performance_diagnostics(tmp_path, trade_date="2026-06-05")
    row = result.frame.iloc[0]

    assert row["benchmark_source"] == "正式加權報酬指數"
    assert row["strategy_history_days"] == 6
    assert row["valid_trade_count"] == 0
    assert bool(row["can_judge_alpha"]) is False
    assert bool(row["can_judge_strategy_alpha"]) is False
    assert pd.isna(row["alpha"])
    assert row["conclusion_status"] == "NOT_ENOUGH_STRATEGY_HISTORY"
    assert "NOT_ENOUGH_STRATEGY_HISTORY" in row["data_quality_warning"]


def test_performance_diagnostics_does_not_trust_market_regime_index_without_official_data(tmp_path: Path) -> None:
    _write_pnl_chart(tmp_path)
    _write_market_regime(tmp_path)

    result = generate_performance_diagnostics(tmp_path, trade_date="2026-06-05")
    row = result.frame.iloc[0]

    assert row["benchmark_source"] == "benchmark 資料不足"
    assert pd.isna(row["benchmark_return"])
    assert pd.isna(row["alpha"])
    assert "缺少正式加權 / 櫃買指數資料" in row["benchmark_warning"]


def test_performance_diagnostics_rejects_abnormal_0050_fallback(tmp_path: Path) -> None:
    _write_pnl_chart(tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.2826,
                "stock_return_20d": "",
                "market_return_5d": "",
                "market_return_20d": "",
            }
        ]
    ).to_csv(tmp_path / "sector_strength.csv", index=False, encoding="utf-8-sig")

    result = generate_performance_diagnostics(tmp_path, trade_date="2026-06-05")
    row = result.frame.iloc[0]

    assert row["benchmark_source"] == "benchmark 資料不足"
    assert "0050 fallback 5d 報酬" in row["benchmark_warning"]
    assert pd.isna(row["alpha"])


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


def _write_market_indices(path: Path) -> None:
    pd.DataFrame(
        [
            {"trade_date": "2026-05-29", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "close": 100.0, "is_official": True},
            {"trade_date": "2026-06-01", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "close": 100.5, "is_official": True},
            {"trade_date": "2026-06-02", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "close": 101.0, "is_official": True},
            {"trade_date": "2026-06-03", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "close": 101.5, "is_official": True},
            {"trade_date": "2026-06-04", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "close": 101.8, "is_official": True},
            {"trade_date": "2026-06-05", "index_id": "TAIEX_TR", "index_name": "發行量加權報酬指數", "close": 102.0, "is_official": True},
        ]
    ).to_csv(path / "market_indices.csv", index=False, encoding="utf-8-sig")


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


def _write_valid_trades(path: Path, count: int) -> None:
    rows = []
    for index in range(count):
        rows.append(
            {
                "trade_date": "2026-06-05",
                "stock_id": f"23{index:02d}",
                "stock_name": f"測試{index}",
                "entry_price": 100 + index,
                "shares": 1000,
                "status": "CLOSED",
                "realized_pnl_pct_after_cost": 0.01,
            }
        )
    pd.DataFrame(rows).to_csv(path / "paper_trades.csv", index=False, encoding="utf-8-sig")


def _write_portfolio(path: Path, count: int) -> None:
    rows = []
    for index in range(count):
        rows.append(
            {
                "trade_date": "2026-06-05",
                "stock_id": f"24{index:02d}",
                "stock_name": f"持倉{index}",
                "status": "OPEN",
                "remaining_shares": 100,
            }
        )
    pd.DataFrame(rows).to_csv(path / "paper_portfolio_20260605.csv", index=False, encoding="utf-8-sig")
