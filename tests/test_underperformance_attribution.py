from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report
from tw_quant.reporting.underperformance_attribution import (
    UNDERPERFORMANCE_ATTRIBUTION_COLUMNS,
    generate_underperformance_attribution,
)


def test_underperformance_attribution_no_data_does_not_crash(tmp_path: Path) -> None:
    result = generate_underperformance_attribution(tmp_path, trade_date="2026-06-05")

    assert list(result.frame.columns) == UNDERPERFORMANCE_ATTRIBUTION_COLUMNS
    assert result.frame.iloc[0]["diagnostic_status"] == "DATA_INSUFFICIENT"
    assert (tmp_path / "underperformance_attribution_20260605.csv").exists()


def test_underperformance_attribution_calculates_cash_drag_and_drawdown(tmp_path: Path) -> None:
    _write_market_indices(tmp_path)
    _write_sector_strength(tmp_path)
    _write_paper_summary(tmp_path)
    _write_paper_trades(tmp_path)
    _write_loss_attribution(tmp_path)
    _write_performance(tmp_path)

    result = generate_underperformance_attribution(tmp_path, trade_date="2026-06-05")
    frame = result.frame

    assert list(frame.columns) == UNDERPERFORMANCE_ATTRIBUTION_COLUMNS
    assert (frame["attribution_type"] == "cash_drag").any()
    assert (frame["attribution_type"] == "drawdown_contribution").any()
    cash_drag = frame[frame["attribution_type"] == "cash_drag"].iloc[0]
    assert cash_drag["cash_ratio"] == 0.7
    assert cash_drag["alpha"] < 0
    drag = frame[frame["attribution_type"] == "drawdown_contribution"].iloc[0]
    assert drag["stock_id"] == "2222"
    assert result.status == "OK_WITH_WARNINGS"


def test_html_report_shows_underperformance_attribution(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "attribution_type": "cash_drag",
                "diagnostic_item": "現金拖累",
                "cash_ratio": 0.7,
                "benchmark_value": 0.08,
                "alpha": -0.056,
                "diagnostic_status": "OBSERVATION_ONLY",
                "conclusion": "現金比例過高可能拖累上漲行情參與度。",
                "data_quality_warning": "OBSERVATION_ONLY",
                "notes": "此區只做診斷，不修改策略。",
            },
            {
                "trade_date": "2026-06-05",
                "attribution_type": "drawdown_contribution",
                "diagnostic_item": "最大回撤股票",
                "stock_id": "2222",
                "stock_name": "測試拖累",
                "drawdown_contribution": -0.12,
                "pnl_after_cost": -12000,
                "diagnostic_status": "OBSERVATION_ONLY",
                "conclusion": "少數股票造成較大回撤。",
                "data_quality_warning": "OBSERVATION_ONLY",
                "notes": "此區只做診斷。",
            },
        ],
        columns=UNDERPERFORMANCE_ATTRIBUTION_COLUMNS,
    ).to_csv(tmp_path / "underperformance_attribution_20260605.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame([{"trade_date": "2026-06-05", "conclusion_status": "UNDERPERFORMING"}]).to_csv(
        tmp_path / "performance_diagnostics_20260605.csv",
        index=False,
        encoding="utf-8-sig",
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "輸大盤歸因" in html
    assert "此區只做輸大盤原因診斷" in html
    assert "現金比例過高可能拖累上漲行情參與度" in html
    assert "AI 已打敗大盤" not in html


def _write_market_indices(path: Path) -> None:
    rows = []
    for index, trade_date in enumerate(pd.bdate_range("2026-05-08", periods=21)):
        rows.append(
            {
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "index_id": "TAIEX",
                "index_name": "發行量加權股價指數",
                "open": 100 + index,
                "high": 100 + index,
                "low": 100 + index,
                "close": 100 + index,
                "source": "test",
                "is_official": True,
            }
        )
    pd.DataFrame(rows).to_csv(path / "market_indices.csv", index=False, encoding="utf-8-sig")


def _write_sector_strength(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "stock_id": "1111",
                "stock_name": "測試貢獻",
                "industry": "電子",
                "stock_return_5d": 0.04,
                "stock_return_20d": 0.05,
                "sector_return_20d": 0.03,
            },
            {
                "trade_date": "2026-06-05",
                "stock_id": "2222",
                "stock_name": "測試拖累",
                "industry": "傳產",
                "stock_return_5d": -0.04,
                "stock_return_20d": -0.10,
                "sector_return_20d": -0.02,
            },
        ]
    ).to_csv(path / "sector_strength.csv", index=False, encoding="utf-8-sig")


def _write_paper_summary(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-05",
                "total_equity_after_cost": 1_000_000,
                "market_value": 300_000,
                "cash": 700_000,
            }
        ]
    ).to_csv(path / "paper_summary_20260605.csv", index=False, encoding="utf-8-sig")


def _write_paper_trades(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-01",
                "stock_id": "1111",
                "stock_name": "測試貢獻",
                "status": "OPEN",
                "market_value": 100_000,
                "unrealized_pnl": 5000,
                "unrealized_pnl_pct": 0.05,
                "exit_reason": "",
            },
            {
                "trade_date": "2026-06-01",
                "stock_id": "2222",
                "stock_name": "測試拖累",
                "status": "CLOSED",
                "realized_pnl_after_cost": -12000,
                "realized_pnl_pct_after_cost": -0.12,
                "exit_reason": "stop_loss",
            },
        ]
    ).to_csv(path / "paper_trades.csv", index=False, encoding="utf-8-sig")


def _write_loss_attribution(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "stock_id": "2222",
                "stock_name": "測試拖累",
                "realized_pnl_pct": -0.12,
                "max_adverse_excursion": -0.15,
                "realized_pnl_after_cost": -12000,
                "exit_reason": "stop_loss",
            }
        ]
    ).to_csv(path / "loss_attribution_20260605.csv", index=False, encoding="utf-8-sig")


def _write_performance(path: Path) -> None:
    pd.DataFrame([{"trade_date": "2026-06-05", "conclusion_status": "UNDERPERFORMING"}]).to_csv(
        path / "performance_diagnostics_20260605.csv",
        index=False,
        encoding="utf-8-sig",
    )
