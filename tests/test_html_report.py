from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report


def test_generate_html_report_creates_index_with_chinese_content(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert output_path.exists()
    assert "台股紙上交易每日報表" in html
    assert "今日重點結論" in html
    assert "系統健康檢查" in html
    assert "系統狀態總覽" in html
    assert "基本面摘要" in html
    assert "今日候選股" in html
    assert "通過風控股票" in html
    assert "待進場清單" in html
    assert "已成交持倉" in html
    assert "紙上交易績效" in html
    assert "交易成本摘要" in html
    assert "出場策略摘要" in html
    assert "最近每日 summary" in html
    assert "非交易日替代交易日說明" in html


def test_generate_html_report_creates_docs_index_for_github_pages(tmp_path: Path) -> None:
    docs_dir = tmp_path / "docs"
    _write_reports(tmp_path)

    reports_index = generate_html_report(tmp_path, docs_dir=docs_dir)
    docs_index = docs_dir / "index.html"
    docs_html = docs_index.read_text(encoding="utf-8")

    assert docs_index.exists()
    assert docs_html == reports_index.read_text(encoding="utf-8")
    assert "台股紙上交易每日報表" in docs_html
    assert "系統狀態總覽" in docs_html
    assert 'lang="zh-Hant"' in docs_html


def test_generate_html_report_translates_fallback_status(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "成功，使用最近有效交易日" in html
    assert "無交易資料" in html
    assert "今日無交易資料，已使用最近有效交易日" in html
    assert "等待進場" in html
    assert "已有持倉，略過重複進場" in html


def test_generate_html_report_does_not_show_raw_english_field_names(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    raw_field_names = [
        "trade_date",
        "requested_date",
        "fallback_date",
        "fallback_reason",
        "scored_rows",
        "candidate_rows",
        "risk_pass_rows",
        "open_positions",
        "closed_positions",
        "unrealized_pnl",
        "realized_pnl",
        "total_equity",
        "total_score",
        "trend_score",
        "momentum_score",
        "risk_score",
    ]
    assert not any(field_name in html for field_name in raw_field_names)


def test_generate_html_report_handles_missing_data_with_chinese_messages(tmp_path: Path) -> None:
    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "目前尚無每日 summary" in html
    assert "目前尚無候選股資料" in html
    assert "目前尚無待進場資料" in html
    assert "目前尚無已成交持倉" in html
    assert "目前尚無已平倉交易" in html


def _write_reports(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "requested_date": "2026-05-10",
                "trade_date": "2026-05-08",
                "fallback_date": "2026-05-08",
                "fallback_reason": "no trading data",
                "scored_rows": 1328,
                "candidate_rows": 20,
                "risk_pass_rows": 6,
                "open_positions": 6,
                "closed_positions": 0,
                "unrealized_pnl": 1234.0,
                "realized_pnl": 0.0,
                "total_equity": 1_001_234.0,
                "total_cost": 123.0,
                "realized_pnl_after_cost": -123.0,
                "total_equity_after_cost": 1_001_111.0,
                "take_profit_exits": 1,
                "stop_loss_exits": 0,
                "trailing_stop_exits": 0,
                "trend_exit_exits": 0,
                "realized_pnl_after_cost_today": 100.0,
                "fundamental_positive_candidates": 1,
                "fundamental_warning_candidates": 0,
                "status": "OK_WITH_FALLBACK",
            }
        ]
    ).to_csv(path / "daily_summary_20260510.csv", index=False, encoding="utf-8-sig")

    candidates = pd.DataFrame(
        [
            {
                "rank": 1,
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "close": 1000.0,
                "total_score": 88.12,
                "trend_score": 90.0,
                "momentum_score": 86.0,
                "fundamental_score": 70.0,
                "chip_score": 60.0,
                "risk_score": 92.0,
                "revenue_yoy": 25.0,
                "revenue_mom": 3.0,
                "accumulated_revenue_yoy": 12.0,
                "fundamental_reason": "月營收年增率大於 20%",
                "is_candidate": 1,
                "risk_pass": 1,
                "risk_reason": "通過風控",
                "reason": "趨勢向上",
                "stop_loss_price": 920.0,
                "suggested_position_pct": 0.1,
            }
        ]
    )
    candidates.to_csv(path / "candidates_20260508.csv", index=False, encoding="utf-8-sig")
    candidates.to_csv(path / "risk_pass_candidates_20260508.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "signal_date": "2026-05-08",
                "planned_entry_date": "NEXT_AVAILABLE_TRADING_DAY",
                "actual_entry_date": "",
                "stock_id": "2330",
                "stock_name": "台積電",
                "signal_close": 1000.0,
                "entry_price": "",
                "entry_price_source": "",
                "shares": "",
                "position_value": "",
                "status": "PENDING",
                "skipped_reason": "",
                "warning": "",
            },
            {
                "signal_date": "2026-05-01",
                "planned_entry_date": "NEXT_AVAILABLE_TRADING_DAY",
                "actual_entry_date": "",
                "stock_id": "2317",
                "stock_name": "鴻海",
                "signal_close": 150.0,
                "entry_price": "",
                "entry_price_source": "",
                "shares": "",
                "position_value": "",
                "status": "SKIPPED_EXISTING_POSITION",
                "skipped_reason": "已有未平倉持倉，略過重複進場",
                "warning": "",
            }
        ]
    ).to_csv(path / "pending_orders_20260508.csv", index=False, encoding="utf-8-sig")

    pd.DataFrame(
        [
            {
                "signal_date": "2026-05-08",
                "actual_entry_date": "2026-05-09",
                "entry_price_source": "OPEN",
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "entry_price": 1000.0,
                "shares": 100,
                "original_shares": 100,
                "remaining_shares": 50,
                "position_value": 100000.0,
                "entry_slippage": 1.0,
                "entry_commission": 20.0,
                "exit_slippage": "",
                "exit_commission": "",
                "exit_tax": "",
                "total_cost": 20.0,
                "realized_pnl_after_cost": "",
                "realized_pnl_pct_after_cost": "",
                "partial_exit_1_done": True,
                "partial_exit_2_done": False,
                "highest_price_since_entry": 1100.0,
                "highest_pnl_pct_since_entry": 0.1,
                "trailing_stop_price": 1034.0,
                "stop_loss_price": 920.0,
                "suggested_position_pct": 0.1,
                "status": "OPEN",
                "current_price": 1010.0,
                "market_value": 101000.0,
                "unrealized_pnl": 1000.0,
                "unrealized_pnl_pct": 0.01,
                "holding_days": 1,
                "stop_loss_hit": False,
                "exit_date": "",
                "exit_price": "",
                "realized_pnl": "",
                "realized_pnl_pct": "",
                "exit_reason": "",
            }
        ]
    ).to_csv(path / "paper_trades.csv", index=False, encoding="utf-8-sig")

    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "total_capital": 1_000_000.0,
                "invested_value": 100_000.0,
                "market_value": 101_000.0,
                "cash": 900_000.0,
                "unrealized_pnl": 1000.0,
                "realized_pnl": 0.0,
                "total_equity": 1_001_000.0,
                "total_cost": 20.0,
                "realized_pnl_after_cost": 0.0,
                "total_equity_after_cost": 1_000_980.0,
                "open_positions": 1,
                "closed_positions": 0,
            }
        ]
    ).to_csv(path / "paper_summary_20260508.csv", index=False, encoding="utf-8-sig")
