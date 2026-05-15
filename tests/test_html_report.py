from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report


def test_generate_html_report_creates_index_with_chinese_content(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert output_path.exists()
    assert "台股紙上交易帳務" in html
    assert "損益總覽" in html
    assert "系統健康檢查" in html
    assert "基本面摘要" in html
    assert "多因子分數摘要" in html or "多因子資料更新狀態" in html
    assert "高風險事件警告數" in html
    assert "估值警告候選股數" in html
    assert "財報警告候選股數" in html
    assert "籌碼加分候選股數" in html
    assert "今日候選股" in html
    assert "通過風控股票" in html
    assert "待進場" in html
    assert "持倉" in html
    assert "已出場" in html
    assert "紙上交易績效" in html
    assert "交易成本摘要" in html
    assert "國泰電子下單手續費率" in html
    assert "0.399‰" in html
    assert "最低手續費" in html
    assert "1 元" in html
    assert "股票交易稅" in html
    assert "0.3%" in html
    assert "ETF 交易稅" in html
    assert "0.1%" in html
    assert "債券 ETF 交易稅" in html
    assert "滑價不是券商費用" in html
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
    assert "台股紙上交易帳務" in docs_html
    assert "損益總覽" in docs_html
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
    assert "目前尚無持倉" in html
    assert "目前尚無已出場交易" in html


def test_generate_html_report_uses_broker_app_cards_and_profit_classes(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert 'class="section-tabs tab-nav"' in html
    assert 'data-tab-target="overview"' in html
    assert 'data-tab-target="positions"' in html
    assert 'data-tab-target="pending"' in html
    assert 'data-tab-target="closed"' in html
    assert 'data-tab-target="fundamental"' in html
    assert 'data-tab-target="health"' in html
    assert 'data-tab-panel="overview"' in html
    assert 'data-tab-panel="positions"' in html
    assert 'document.querySelectorAll' in html
    assert '<details class="collapse-block"' in html
    assert "<summary>今日候選股詳細表</summary>" in html
    assert "<summary>通過風控股票詳細表</summary>" in html
    assert "<summary>最近每日 summary</summary>" in html
    assert "profit-positive" in html
    assert "profit-negative" in html
    assert "position-pnl pnl-highlight profit-positive" in html
    assert "closed-pnl pnl-highlight profit-negative" in html
    assert "mobile-card position-card" in html
    assert "pending-card" in html
    assert "closed-card" in html
    assert "持有中" in html
    assert "已出場" in html
    assert "等待進場" in html
    assert "已有持倉，略過重複進場" in html


def test_generate_html_report_translates_all_exit_reasons(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "停損" in html
    assert "第一段停利" in html
    assert "第二段停利" in html
    assert "移動停利" in html
    assert "跌破 20 日均線" in html
    assert "持有過久出場" in html


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
                "multi_factor_data_status": "OK:5",
                "high_risk_event_candidates": 1,
                "valuation_warning_candidates": 1,
                "financial_warning_candidates": 1,
                "institutional_positive_candidates": 1,
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
                "original_total_score": 88.12,
                "multi_factor_score": 84.3,
                "multi_factor_reason": "原始技術/動能分數 88.12；月營收年增率大於 20%",
                "trend_score": 90.0,
                "momentum_score": 86.0,
                "fundamental_score": 70.0,
                "chip_score": 60.0,
                "risk_score": 92.0,
                "revenue_score": 80.0,
                "revenue_yoy": 25.0,
                "revenue_mom": 3.0,
                "accumulated_revenue_yoy": 12.0,
                "revenue_reason": "月營收年增率大於 20%",
                "fundamental_reason": "月營收年增率大於 20%",
                "valuation_score": 45.0,
                "pe_ratio": 45.0,
                "pb_ratio": 6.0,
                "dividend_yield": 1.5,
                "valuation_reason": "PE 過高扣分",
                "valuation_warning": "PE 偏高",
                "financial_score": 42.0,
                "eps": 1.2,
                "roe": 8.0,
                "gross_margin": 30.0,
                "operating_margin": 10.0,
                "debt_ratio": 65.0,
                "financial_reason": "負債比過高扣分",
                "financial_warning": "負債比偏高",
                "event_score": 20.0,
                "event_reason": "高風險重大訊息：資安事件",
                "event_risk_level": "HIGH",
                "event_blocked": True,
                "institutional_score": 70.0,
                "foreign_net_buy": 100.0,
                "investment_trust_net_buy": 50.0,
                "dealer_net_buy": 10.0,
                "institutional_reason": "投信連買",
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
            },
            {
                "signal_date": "2026-05-01",
                "actual_entry_date": "2026-05-02",
                "entry_price_source": "OPEN",
                "trade_date": "2026-05-02",
                "stock_id": "2317",
                "stock_name": "鴻海",
                "entry_price": 150.0,
                "shares": 100,
                "original_shares": 100,
                "remaining_shares": 0,
                "position_value": 0.0,
                "entry_slippage": 0.1,
                "entry_commission": 20.0,
                "exit_slippage": 0.1,
                "exit_commission": 20.0,
                "exit_tax": 45.0,
                "total_cost": 95.0,
                "realized_pnl_after_cost": -500.0,
                "realized_pnl_pct_after_cost": -0.03,
                "partial_exit_1_done": False,
                "partial_exit_2_done": False,
                "highest_price_since_entry": 152.0,
                "highest_pnl_pct_since_entry": 0.01,
                "trailing_stop_price": "",
                "stop_loss_price": 142.0,
                "suggested_position_pct": 0.1,
                "status": "CLOSED",
                "current_price": 145.0,
                "market_value": 0.0,
                "unrealized_pnl": 0.0,
                "unrealized_pnl_pct": 0.0,
                "holding_days": 3,
                "stop_loss_hit": True,
                "exit_date": "2026-05-05",
                "exit_price": 145.0,
                "realized_pnl": -500.0,
                "realized_pnl_pct": -0.03,
                "exit_reason": "STOP_LOSS",
            },
            {
                "trade_date": "2026-05-02",
                "stock_id": "2454",
                "stock_name": "聯發科",
                "entry_price": 900.0,
                "shares": 10,
                "original_shares": 10,
                "remaining_shares": 0,
                "position_value": 0.0,
                "total_cost": 90.0,
                "realized_pnl_after_cost": 900.0,
                "realized_pnl_pct_after_cost": 0.1,
                "status": "CLOSED",
                "exit_date": "2026-05-06",
                "exit_price": 990.0,
                "realized_pnl": 900.0,
                "realized_pnl_pct": 0.1,
                "exit_reason": "TAKE_PROFIT_1",
            },
            {
                "trade_date": "2026-05-02",
                "stock_id": "2308",
                "stock_name": "台達電",
                "entry_price": 300.0,
                "shares": 20,
                "original_shares": 20,
                "remaining_shares": 0,
                "position_value": 0.0,
                "total_cost": 80.0,
                "realized_pnl_after_cost": 1200.0,
                "realized_pnl_pct_after_cost": 0.2,
                "status": "CLOSED",
                "exit_date": "2026-05-06",
                "exit_price": 360.0,
                "realized_pnl": 1200.0,
                "realized_pnl_pct": 0.2,
                "exit_reason": "TAKE_PROFIT_2",
            },
            {
                "trade_date": "2026-05-02",
                "stock_id": "2382",
                "stock_name": "廣達",
                "entry_price": 250.0,
                "shares": 20,
                "original_shares": 20,
                "remaining_shares": 0,
                "position_value": 0.0,
                "total_cost": 70.0,
                "realized_pnl_after_cost": 500.0,
                "realized_pnl_pct_after_cost": 0.1,
                "status": "CLOSED",
                "exit_date": "2026-05-06",
                "exit_price": 275.0,
                "realized_pnl": 500.0,
                "realized_pnl_pct": 0.1,
                "exit_reason": "TRAILING_STOP",
            },
            {
                "trade_date": "2026-05-02",
                "stock_id": "2881",
                "stock_name": "富邦金",
                "entry_price": 80.0,
                "shares": 100,
                "original_shares": 100,
                "remaining_shares": 0,
                "position_value": 0.0,
                "total_cost": 60.0,
                "realized_pnl_after_cost": -200.0,
                "realized_pnl_pct_after_cost": -0.025,
                "status": "CLOSED",
                "exit_date": "2026-05-06",
                "exit_price": 78.0,
                "realized_pnl": -200.0,
                "realized_pnl_pct": -0.025,
                "exit_reason": "MA_EXIT",
            },
            {
                "trade_date": "2026-05-02",
                "stock_id": "1101",
                "stock_name": "台泥",
                "entry_price": 40.0,
                "shares": 100,
                "original_shares": 100,
                "remaining_shares": 0,
                "position_value": 0.0,
                "total_cost": 40.0,
                "realized_pnl_after_cost": 50.0,
                "realized_pnl_pct_after_cost": 0.0125,
                "status": "CLOSED",
                "exit_date": "2026-05-06",
                "exit_price": 40.5,
                "realized_pnl": 50.0,
                "realized_pnl_pct": 0.0125,
                "exit_reason": "TIME_EXIT",
            },
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


def test_generate_html_report_creates_index_with_chinese_content(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert output_path.exists()
    assert "台股紙上交易帳務" in html
    assert "今日重點結論" in html
    assert "損益總覽" in html
    assert "目前持倉" in html
    assert "待進場" in html
    assert "今日 / 最近已出場" in html
    assert "基本面摘要" in html
    assert "市場判斷摘要" in html
    assert "系統健康檢查" in html
    assert "交易成本摘要" in html


def test_generate_html_report_creates_docs_index_for_github_pages(tmp_path: Path) -> None:
    docs_dir = tmp_path / "docs"
    _write_reports(tmp_path)

    reports_index = generate_html_report(tmp_path, docs_dir=docs_dir)
    docs_index = docs_dir / "index.html"
    docs_html = docs_index.read_text(encoding="utf-8")

    assert docs_index.exists()
    assert docs_html == reports_index.read_text(encoding="utf-8")
    assert "台股紙上交易帳務" in docs_html
    assert 'lang="zh-Hant"' in docs_html


def test_generate_html_report_translates_fallback_status(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "成功，使用最近有效交易日" in html
    assert "無交易資料" in html
    assert "等待進場" in html
    assert "已有持倉，略過重複進場" in html


def test_generate_html_report_does_not_show_raw_english_field_names(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    visible_raw_field_names = [
        ">trade_date<",
        ">requested_date<",
        ">fallback_date<",
        ">candidate_rows<",
        ">risk_pass_rows<",
        ">total_score<",
    ]
    assert not any(field_name in html for field_name in visible_raw_field_names)


def test_generate_html_report_handles_missing_data_with_chinese_messages(tmp_path: Path) -> None:
    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "今日無重點結論資料" in html
    assert "今日無市場判斷資料" in html
    assert "目前尚無紙上交易紀錄" in html or "今日無資料" in html


def test_generate_html_report_uses_broker_app_cards_and_profit_classes(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert 'class="section-tabs tab-nav"' in html
    assert 'data-tab-target="overview"' in html
    assert 'data-tab-target="positions"' in html
    assert 'data-tab-target="pending"' in html
    assert 'data-tab-target="closed"' in html
    assert '<details class="collapse-block"' in html
    assert "profit-positive" in html
    assert "profit-negative" in html
    assert "positive" in html
    assert "negative" in html
    assert "mobile-card position-card" in html
    assert "pending-card" in html
    assert "closed-card" in html
    assert "持有中" in html
    assert "已出場" in html
    assert "等待進場" in html
    assert "已有持倉，略過重複進場" in html


def test_generate_html_report_translates_all_exit_reasons(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "停損" in html
    assert "第一段停利" in html
    assert "第二段停利" in html
    assert "移動停利" in html
    assert "跌破 20 日均線" in html
    assert "持有過久出場" in html
