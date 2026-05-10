from __future__ import annotations

import pandas as pd
import pytest

from tw_quant.reporting.performance import SUMMARY_COLUMNS, load_paper_performance


def test_load_paper_performance_calculates_metrics(tmp_path) -> None:
    _write_trades(tmp_path)
    _write_summary(tmp_path / "paper_summary_20260508.csv", "2026-05-08", 1000, 0, 0, 1, 0)
    _write_summary(tmp_path / "paper_summary_20260509.csv", "2026-05-09", 1100, 100, 0, 1, 0)
    _write_summary(tmp_path / "paper_summary_20260510.csv", "2026-05-10", 1045, 45, 5, 1, 2)

    performance = load_paper_performance(tmp_path, capital=1000)

    assert performance.warning == ""
    assert list(performance.summary.columns) == SUMMARY_COLUMNS
    assert len(performance.summary) == 3
    assert len(performance.open_positions) == 1
    assert len(performance.closed_trades) == 2
    assert performance.metrics["open_positions"] == 1
    assert performance.metrics["closed_positions"] == 2
    assert performance.metrics["win_rate"] == 0.5
    assert performance.metrics["total_return_pct"] == pytest.approx(0.045)
    assert round(performance.metrics["max_drawdown"], 4) == -0.05


def test_load_paper_performance_uses_daily_summary_when_paper_summary_missing(tmp_path) -> None:
    _write_trades(tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "scored_rows": 1328,
                "candidate_rows": 20,
                "risk_pass_rows": 6,
                "new_positions": 6,
                "open_positions": 6,
                "closed_positions": 0,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "total_equity": 1000.0,
            }
        ]
    ).to_csv(tmp_path / "daily_summary_20260508.csv", index=False)

    performance = load_paper_performance(tmp_path, capital=1000)

    assert performance.warning == ""
    assert len(performance.summary) == 1
    assert performance.summary.iloc[0]["total_equity"] == 1000.0


def test_load_paper_performance_prefers_paper_summary_over_daily_summary(tmp_path) -> None:
    _write_trades(tmp_path)
    _write_summary(tmp_path / "daily_summary_20260508.csv", "2026-05-08", 900, -100, 0, 1, 0)
    _write_summary(tmp_path / "paper_summary_20260508.csv", "2026-05-08", 1000, 0, 0, 1, 0)

    performance = load_paper_performance(tmp_path, capital=1000)

    assert len(performance.summary) == 1
    assert performance.summary.iloc[0]["total_equity"] == 1000


def test_load_paper_performance_warns_when_reports_missing(tmp_path) -> None:
    performance = load_paper_performance(tmp_path, capital=1000)

    assert performance.warning == "paper trading reports not found"
    assert performance.summary.empty
    assert performance.open_positions.empty
    assert performance.closed_trades.empty


def _write_trades(path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "entry_price": 100,
                "shares": 10,
                "position_value": 1000,
                "stop_loss_price": 90,
                "suggested_position_pct": 0.1,
                "status": "OPEN",
                "realized_pnl": 0,
            },
            {
                "trade_date": "2026-05-08",
                "stock_id": "2317",
                "stock_name": "鴻海",
                "entry_price": 50,
                "shares": 10,
                "position_value": 500,
                "stop_loss_price": 45,
                "suggested_position_pct": 0.05,
                "status": "CLOSED",
                "realized_pnl": 100,
            },
            {
                "trade_date": "2026-05-08",
                "stock_id": "2454",
                "stock_name": "聯發科",
                "entry_price": 1000,
                "shares": 1,
                "position_value": 1000,
                "stop_loss_price": 900,
                "suggested_position_pct": 0.1,
                "status": "CLOSED",
                "realized_pnl": -50,
            },
        ]
    ).to_csv(path / "paper_trades.csv", index=False, encoding="utf-8-sig")


def _write_summary(
    path,
    trade_date: str,
    total_equity: float,
    unrealized_pnl: float,
    realized_pnl: float,
    open_positions: int,
    closed_positions: int,
) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "total_capital": 1000,
                "invested_value": 0,
                "market_value": 0,
                "cash": total_equity,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": realized_pnl,
                "total_equity": total_equity,
                "open_positions": open_positions,
                "closed_positions": closed_positions,
            }
        ]
    ).to_csv(path, index=False, encoding="utf-8-sig")
