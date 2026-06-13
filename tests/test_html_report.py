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
    assert "資料健康檢查" in html
    assert "資料不足只代表需要補查或使用 fallback" in html
    assert (tmp_path / "data_quality_health.csv").exists()
    assert "市場情報 / 多因子" in html
    assert "決策引擎" in html
    assert "BUY_CANDIDATE" in html
    assert "can_auto_trade=false" in html
    assert "保證獲利" not in html
    assert "一定買進" not in html
    assert "一定賣出" not in html
    assert "<summary>完整 trading_decisions 原始表格</summary>" in html
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


def test_generate_html_report_shows_market_intel_cache_warning(tmp_path: Path) -> None:
    _write_reports(tmp_path)
    pd.DataFrame(
        [
            {
                "source_name": "market_intel",
                "status": "CACHE",
                "rows": 1,
                "warning": "使用快取 / 非當日資料",
                "requested_period": "2026-05-28",
                "actual_period": "2026-05-27",
                "latest_available_period": "2026-05-27",
                "is_real_data": True,
                "is_mock": False,
                "is_stale": True,
                "data_age_days": 1,
                "data_freshness_level": "CACHE",
                "affected_symbols_count": 1,
            }
        ]
    ).to_csv(tmp_path / "data_fetch_status_20260527.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "market_intel_status": "CACHE",
                "requested_date": "2026-05-28",
                "actual_data_date": "2026-05-27",
                "fallback_date": "2026-05-27",
                "fallback_reason": "no trading data",
                "cache_age_days": 1,
                "is_stale_data": True,
                "data_freshness_level": "CACHE",
                "market_intel_source": "mixed",
                "market_intel_warning": "使用快取 / 非當日資料",
                "final_market_score": 60,
                "confidence_score": 70,
            }
        ]
    ).to_csv(tmp_path / "market_intel_20260527.csv", index=False, encoding="utf-8")

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "使用快取 / 非當日資料" in html
    assert "目前市場情報使用快取或非當日資料，不建議短線自動進場。" in html
    assert "市場資料過期，不建議短線進場" in html
    assert "資料鮮度等級" in html
    assert "CACHE" in html
    assert "實際資料日" in html
    assert "2026-05-27" in html


def test_generate_html_report_corrects_stale_actual_data_date(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "requested_date": "2026-06-12",
                "trade_date": "2026-06-12",
                "status": "OK",
                "total_capital": 1_000_000,
                "total_equity_after_cost": 1_000_000,
                "actual_data_date": "2026-06-09",
                "cache_age_days": 0,
                "is_stale_data": False,
                "data_freshness_level": "CURRENT",
                "market_intel_status": "OK",
            }
        ]
    ).to_csv(tmp_path / "daily_summary_20260612.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-12",
                "stock_id": "2330",
                "stock_name": "台積電",
                "requested_date": "2026-06-12",
                "actual_data_date": "2026-06-09",
                "cache_age_days": 0,
                "data_freshness_level": "CURRENT",
                "market_intel_status": "OK",
                "final_market_score": 50,
                "confidence_score": 50,
            }
        ]
    ).to_csv(tmp_path / "market_intel_20260612.csv", index=False, encoding="utf-8")

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "實際資料日" in html
    assert "2026-06-09" in html
    assert "3 天" in html
    assert "資料過期" in html
    assert "目前最新交易日資料" not in html


def test_freshness_readiness_dashboard_marks_stale_cache_age_and_windows(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "requested_date": "2026-06-13",
                "trade_date": "2026-06-13",
                "status": "OK_WITH_FALLBACK",
                "total_capital": 1_000_000,
                "total_equity_after_cost": 1_000_000,
                "actual_data_date": "2026-06-09",
                "fallback_date": "2026-06-09",
                "fallback_reason": "no trading data",
                "used_latest_available": True,
                "cache_age_days": 4,
                "is_stale_data": True,
                "data_freshness_level": "STALE",
                "market_intel_status": "CACHE",
            }
        ]
    ).to_csv(tmp_path / "daily_summary_20260613.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-13",
                "primary_alpha_window": "20d",
                "strategy_return_20d": 0.01,
                "benchmark_return_20d": 0.03,
                "excess_return_20d": -0.02,
                "excess_return": -0.02,
                "alpha": -0.02,
                "conclusion_status": "UNDERPERFORMING",
                "risk_adjusted_alpha_status": "UNDERPERFORMING",
                "benchmark_history_days": 180,
                "strategy_history_days": 27,
                "valid_trade_count": 25,
                "holding_record_count": 8,
                "can_judge_alpha": True,
                "can_judge_alpha_20d": True,
                "can_judge_strategy_alpha_5d": True,
                "can_judge_strategy_alpha_20d": True,
                "can_judge_strategy_alpha_60d": False,
                "can_judge_strategy_alpha_120d": False,
                "can_judge_strategy_alpha_252d": False,
            }
        ]
    ).to_csv(tmp_path / "performance_diagnostics_20260613.csv", index=False, encoding="utf-8")

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "資料新鮮度與策略成熟度" in html
    assert "資料非最新交易日" in html
    assert "資料落後 4 天" in html
    assert "使用最近有效資料：是" in html
    assert "無交易資料" in html
    assert "資料最新或可用" not in html
    assert "<td>20d</td><td>可判斷</td>" in html
    assert "<td>60d</td><td>資料不足</td>" in html
    assert "20d primary_excess_return 未高於 0，目前不可顯示打敗大盤" in html
    assert "正式長期打敗大盤：否" in html
    assert "正式長期打敗大盤：是" not in html


def test_freshness_readiness_dashboard_positive_alpha_is_short_term_only(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "requested_date": "2026-06-13",
                "trade_date": "2026-06-13",
                "status": "OK",
                "total_capital": 1_000_000,
                "total_equity_after_cost": 1_020_000,
                "actual_data_date": "2026-06-13",
                "cache_age_days": 0,
                "is_stale_data": False,
                "data_freshness_level": "CURRENT",
                "market_intel_status": "OK",
            }
        ]
    ).to_csv(tmp_path / "daily_summary_20260613.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-06-13",
                "primary_alpha_window": "20d",
                "strategy_return_20d": 0.05,
                "benchmark_return_20d": 0.02,
                "excess_return_20d": 0.03,
                "excess_return": 0.03,
                "alpha": 0.03,
                "conclusion_status": "OUTPERFORMING_SHORT_TERM",
                "risk_adjusted_alpha_status": "OUTPERFORMING_SHORT_TERM",
                "benchmark_history_days": 180,
                "strategy_history_days": 27,
                "valid_trade_count": 25,
                "holding_record_count": 8,
                "can_judge_alpha": True,
                "can_judge_alpha_20d": True,
                "can_judge_strategy_alpha_5d": True,
                "can_judge_strategy_alpha_20d": True,
                "can_judge_strategy_alpha_60d": False,
                "can_judge_strategy_alpha_120d": False,
                "can_judge_strategy_alpha_252d": False,
            }
        ]
    ).to_csv(tmp_path / "performance_diagnostics_20260613.csv", index=False, encoding="utf-8")

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "20d 短期跑贏" in html
    assert "短期觀察，不代表長期打敗大盤" in html
    assert "正式長期打敗大盤：否" in html
    assert "正式長期打敗大盤：是" not in html


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

    assert 'class="account-header brokerage-header"' in html
    assert "TW-Quant Paper Trading" in html
    assert "台股投資儀表板" in html
    assert "header-status-tile" in html
    assert "Brokerage-style dashboard refresh" in html
    assert ".quick-section-nav{display:none}" in html
    assert 'class="section-tabs tab-nav"' in html
    assert 'data-tab-target="overview"' in html
    assert 'data-tab-target="positions"' in html
    assert 'data-tab-target="pending"' in html
    assert 'data-tab-target="closed"' in html
    assert 'data-tab-target="decision"' in html
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


def test_generate_html_report_has_modern_dashboard_sections_and_badges(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "總覽儀表板" in html
    assert "今日市場資料狀態" in html
    assert "交易安全狀態" in html
    assert "紙上交易資產" in html
    assert "今日決策統計" in html
    assert "產業分類缺口" in html
    assert "AnySearch 候選資料" in html
    assert "資料為最新或目前可用資料" in html
    assert 'id="pnl-overview"' in html
    assert 'class="asset-donut-card"' in html
    assert "資產 / 損益圓環卡" in html
    assert "總報酬率" in html
    assert "前幾大持倉" in html
    assert "總成本" in html
    assert "conic-gradient" in html
    assert html.index('id="pnl-overview"') < html.index('id="dashboard-overview"')
    assert html.index('id="pnl-overview"') < html.index("今日損益圖")
    assert ":root{--text-main:#111827;--text-secondary:#374151;--text-muted:#374151;--text-strong:#000000}" in html
    assert "#94a3b8" not in html
    assert "#cbd5e1" not in html
    assert ".action-list li,.quality-list li,.risk-list li,.catalyst-list li{color:var(--text-main)" in html
    assert ".detail-grid dd,.mobile-card dl dd,.card-details dd,.collapse-content dd{color:var(--text-main)" in html
    assert ".card-details summary,.collapse-block summary{color:var(--text-strong)" in html
    assert ".chart-legend,.recap-summary,.pnl-bar-row span,.holding-head span,.card-title-row span{color:var(--text-secondary)" in html
    assert "大盤比較 / 超額報酬" in html
    assert 'id="benchmark-alpha"' in html
    assert "超額報酬 alpha" in html
    assert "策略績效診斷" in html
    assert 'id="strategy-diagnostics"' in html
    assert "最有效因子前 3 名" in html
    assert "Guardrail 影響" in html
    assert "平均 5 日後報酬" in html
    assert "市場環境分數偏低" in html
    assert "Benchmark warning" in html
    assert "market_regime_score 說明" in html
    assert "新增持倉風控分數" in html
    assert "不是選股分數，也不是獲利保證" in html
    assert "5 日市場報酬" in html
    assert "20 日均線站上比例" in html
    assert 'class="status-badge badge-ok freshness-badge"' in html
    assert 'class="status-badge badge-ok guardrail-badge"' in html
    assert 'data-section-target="missing-industry-section"' in html
    assert 'data-section-target="anysearch-candidates-section"' in html
    assert "缺產業分類優先補資料清單" in html
    assert "AnySearch 產業分類候選資料" in html
    assert "PENDING_REVIEW" in html
    assert "NEEDS_MANUAL_CHECK" in html
    assert "候選資料，尚未正式採用" in html
    assert "已正式採用" not in html


def test_benchmark_alpha_uses_0050_fallback_and_shows_summary(tmp_path: Path) -> None:
    _write_reports(tmp_path)
    for index, equity in enumerate([990_000, 992_000, 994_000, 996_000, 998_000], start=3):
        pd.DataFrame(
            [
                {
                    "requested_date": f"2026-05-0{index}",
                    "trade_date": f"2026-05-0{index}",
                    "status": "OK",
                    "total_capital": 1_000_000.0,
                    "total_equity_after_cost": equity,
                    "total_equity": equity,
                    "market_regime_score": 65.0,
                }
            ]
        ).to_csv(tmp_path / f"daily_summary_2026050{index}.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.01,
                "stock_return_20d": 0.02,
                "market_return_5d": 0.008,
                "market_return_20d": 0.015,
            }
        ]
    ).to_csv(tmp_path / "sector_strength.csv", index=False, encoding="utf-8")

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "大盤比較 / 超額報酬" in html
    assert "0050 fallback" in html
    assert "未使用正式加權指數資料" in html
    assert "不能假裝是正式大盤指數" in html
    assert "打敗大盤" in html
    assert "超額報酬 alpha" in html
    assert "大盤比較詳細數據" in html
    assert "近 5 日" in html


def test_asset_donut_falls_back_when_position_allocation_is_missing(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "requested_date": "2026-05-10",
                "trade_date": "2026-05-08",
                "status": "OK",
                "total_capital": 1_000_000.0,
                "total_equity_after_cost": 1_001_000.0,
                "unrealized_pnl": 1_000.0,
                "realized_pnl_after_cost": 0.0,
                "new_entries_allowed": True,
                "guardrail_status": "OK",
                "data_freshness_level": "CURRENT",
                "market_intel_status": "OK",
            }
        ]
    ).to_csv(tmp_path / "daily_summary_20260510.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "total_capital": 1_000_000.0,
                "total_equity_after_cost": 1_001_000.0,
                "unrealized_pnl": 1_000.0,
                "realized_pnl_after_cost": 0.0,
            }
        ]
    ).to_csv(tmp_path / "paper_summary_20260508.csv", index=False, encoding="utf-8")

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "資產 / 損益圓環卡" in html
    assert "持倉資料不足，改顯示損益摘要。" in html
    assert "總資產" in html
    assert "總報酬率" in html


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
                "actual_data_date": "2026-05-08",
                "cache_age_days": 0,
                "is_stale_data": False,
                "data_freshness_level": "CURRENT",
                "market_intel_status": "OK",
                "guardrail_status": "OK",
                "new_entries_allowed": True,
                "pause_new_entries_reason": "",
                "buy_candidate_count": 1,
                "watch_only_count": 0,
                "no_trade_count": 0,
                "hold_count": 1,
                "reduce_count": 0,
                "exit_review_count": 0,
                "market_regime_score": 65.0,
            }
        ]
    ).to_csv(path / "daily_summary_20260510.csv", index=False, encoding="utf-8")

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
    candidates.to_csv(path / "candidates_20260508.csv", index=False, encoding="utf-8")
    candidates.to_csv(path / "risk_pass_candidates_20260508.csv", index=False, encoding="utf-8")
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
    ).to_csv(path / "pending_orders_20260508.csv", index=False, encoding="utf-8")

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
    ).to_csv(path / "paper_trades.csv", index=False, encoding="utf-8")

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
    ).to_csv(path / "paper_summary_20260508.csv", index=False, encoding="utf-8")

    pd.DataFrame(
        [
            {
                "decision_date": "2026-05-08",
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
                "source": "candidate",
                "current_status": "CANDIDATE",
                "decision": "BUY_CANDIDATE",
                "decision_level": "WATCH",
                "action": "review_before_entry",
                "candidate_grade": "A",
                "reason": "買進候選，需人工確認；不會自動下單",
                "risk_flags": "",
                "confidence_score": 80,
                "total_score": 88,
                "multi_factor_score": 84,
                "final_market_score": 82,
                "liquidity_score": 70,
                "sector_strength_score": 60,
                "event_risk_level": "NONE",
                "position_size_suggestion": 0.1,
                "can_auto_trade": False,
                "requires_manual_review": True,
                "data_quality_note": "資料品質已檢查；仍需人工確認",
            }
        ]
    ).to_csv(path / "trading_decisions_20260508.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "validation_date": "2026-05-08",
                "trade_date": "2026-05-08",
                "model_name": "baseline_total_score",
                "description": "原始 total_score / risk_pass baseline",
                "candidate_count": 1,
                "selected_count": 1,
                "simulated_trades": 1,
                "win_rate": 1.0,
                "avg_return_pct": 0.1,
                "median_return_pct": 0.1,
                "total_return_pct": 0.1,
                "max_drawdown_pct": 0.0,
                "avg_holding_days": 5,
                "notes": "",
            }
        ]
    ).to_csv(path / "strategy_validation_20260508.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "market_regime_score": 65.0,
                "source": "index",
                "twse_above_20ma": True,
                "twse_above_60ma": False,
                "market_return_5d": 0.8,
                "market_return_20d": 1.5,
                "market_above_20ma_ratio": 0.55,
                "market_above_60ma_ratio": 0.48,
                "warning": "",
            }
        ]
    ).to_csv(path / "market_regime_20260508.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "market_regime_score": 65.0,
                "regime_label": "大盤中性",
                "twse_index": "",
                "tpex_index": "",
                "advancers": 1,
                "decliners": 1,
                "unchanged": 0,
                "fallback_used": True,
                "data_quality_note": "測試用 fallback",
            }
        ]
    ).to_csv(path / "market_recap_20260508.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "0050",
                "stock_name": "元大台灣50",
                "stock_return_5d": 0.01,
                "stock_return_20d": 0.02,
                "market_return_5d": 0.008,
                "market_return_20d": 0.015,
            }
        ]
    ).to_csv(path / "sector_strength.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "stock_id": "1001",
                "stock_name": "缺產業測試",
                "market_type": "TWSE",
                "latest_relative_mode": "market_relative_fallback",
                "fallback_reason": "缺少產業分類，使用全市場相對強弱",
                "appear_in_candidates_count": 1,
                "appear_in_trading_decisions_count": 1,
                "appear_in_risk_pass_count": 0,
                "appear_in_position_review_count": 0,
                "appear_in_ai_enrichment_count": 0,
                "recent_appearance_count": 2,
                "liquidity_score": 92,
                "avg_volume": 100000,
                "turnover_value": 2500000,
                "last_seen_date": "2026-05-08",
                "priority_score": 14,
                "priority_level": "HIGH",
                "suggested_action": "優先查證並補產業分類",
            },
            {
                "stock_id": "1002",
                "stock_name": "低優先測試",
                "market_type": "TPEX",
                "latest_relative_mode": "market_relative_fallback",
                "fallback_reason": "缺少產業分類，使用全市場相對強弱",
                "recent_appearance_count": 0,
                "priority_score": 1,
                "priority_level": "LOW",
                "suggested_action": "暫緩補資料",
            },
        ]
    ).to_csv(path / "missing_industry_priority.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "stock_id": "00999",
                "stock_name": "測試 ETF",
                "query": "00999 測試 ETF 官方",
                "proposed_market_type": "ETF",
                "proposed_industry": "ETF",
                "proposed_sub_industry": "測試 ETF",
                "source_title": "官方測試來源",
                "source_url": "https://example.com/etf",
                "source_type": "official",
                "confidence": 0.8,
                "reason": "候選資料，等待人工確認",
                "status": "PENDING_REVIEW",
                "checked_at": "2026-05-08T09:00:00",
            },
            {
                "stock_id": "00998",
                "stock_name": "需人工確認 ETF",
                "query": "00998 ETF",
                "proposed_market_type": "ETF",
                "proposed_industry": "ETF",
                "proposed_sub_industry": "",
                "source_title": "待確認來源",
                "source_url": "https://example.com/check",
                "source_type": "web",
                "confidence": 0.4,
                "reason": "缺少官方來源",
                "status": "NEEDS_MANUAL_CHECK",
                "checked_at": "2026-05-08T09:00:00",
            },
        ]
    ).to_csv(path / "anysearch_industry_candidates.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "factor_name": "total_score",
                "bucket": "HIGH",
                "stock_count": 2,
                "trade_count": 1,
                "avg_forward_return_1d": "",
                "avg_forward_return_5d": "",
                "avg_forward_return_20d": "",
                "win_rate_1d": "",
                "win_rate_5d": "",
                "win_rate_20d": "",
                "avg_return_pct": 0.03,
                "median_return_pct": 0.03,
                "total_realized_pnl_after_cost": 3000,
                "avg_realized_pnl_after_cost": 3000,
                "max_loss_pct": -0.01,
                "max_gain_pct": 0.05,
                "benchmark_return_1d": "",
                "benchmark_return_5d": 0.01,
                "benchmark_return_20d": 0.02,
                "alpha_1d": "",
                "alpha_5d": "",
                "alpha_20d": "",
                "conclusion": "DATA_INSUFFICIENT",
                "data_quality_warning": "DATA_INSUFFICIENT",
                "notes": "測試資料",
            }
        ]
    ).to_csv(path / "factor_attribution_20260508.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "report_date": "2026-05-08",
                "factor_name": "total_score",
                "best_bucket": "HIGH",
                "best_alpha_20d": "",
                "best_avg_return_pct": 0.03,
                "worst_bucket": "LOW",
                "worst_alpha_20d": "",
                "worst_avg_return_pct": -0.01,
                "total_stock_count": 2,
                "total_trade_count": 1,
                "sample_status": "DATA_INSUFFICIENT",
                "conclusion": "DATA_INSUFFICIENT",
                "data_quality_warning": "DATA_INSUFFICIENT",
                "notes": "測試摘要",
            }
        ]
    ).to_csv(path / "factor_attribution_summary_20260508.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "system_cumulative_return": 0.001,
                "benchmark_cumulative_return": "",
                "alpha": "",
                "alpha_1d": "",
                "alpha_5d": -0.002,
                "alpha_20d": "",
                "win_rate_vs_benchmark": 0,
                "max_drawdown": -0.01,
                "benchmark_source": "0050 fallback",
                "benchmark_warning": "測試 benchmark fallback",
                "data_quality_warning": "DATA_INSUFFICIENT",
                "notes": "測試 benchmark",
            }
        ]
    ).to_csv(path / "benchmark_diagnostics_20260508.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "rejected_reason": "市場環境分數偏低",
                "rejected_count": 1,
                "avg_forward_return_1d": "",
                "avg_forward_return_5d": "",
                "avg_forward_return_20d": "",
                "missed_winner_count": 0,
                "avoided_loser_count": 0,
                "estimated_alpha_impact": "",
                "notes": "DATA_INSUFFICIENT",
            }
        ]
    ).to_csv(path / "guardrail_impact_20260508.csv", index=False, encoding="utf-8")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "source": "pnl_chart_data",
                "observation_start": "2026-05-01",
                "observation_end": "2026-05-08",
                "observation_count": 6,
                "daily_return_count": 5,
                "cumulative_return": 0.04,
                "annualized_return": 0.12,
                "volatility": 0.18,
                "sharpe_like_ratio": 1.23,
                "sortino_like_ratio": 1.56,
                "max_drawdown": -0.02,
                "win_rate_by_day": 0.6,
                "best_day": "2026-05-07",
                "best_day_return": 0.03,
                "worst_day": "2026-05-03",
                "worst_day_return": -0.02,
                "profit_factor": 1.8,
                "benchmark_return": 0.01,
                "benchmark_window": "5d",
                "alpha": 0.03,
                "benchmark_source": "加權指數",
                "benchmark_warning": "",
                "status": "OK",
                "data_quality_warning": "",
                "notes": "測試績效診斷",
            }
        ]
    ).to_csv(path / "performance_diagnostics_20260508.csv", index=False, encoding="utf-8")


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
    assert "市場情報 / 多因子" in html
    assert "市場判斷摘要" in html
    assert "系統健康檢查" in html
    assert "交易成本摘要" in html
    assert "績效風險分析" in html
    assert "風險調整後 Alpha" in html
    assert "尚不可正式宣稱 AI 長期打敗大盤" in html
    assert 'id="performance-diagnostics"' in html
    assert "Sharpe-like" in html
    assert "績效風險診斷明細" in html
    assert "strategy_history_days" in html
    assert "valid_trade_count" in html
    assert "can_judge_strategy_alpha" in html
    assert "conclusion_status" in html


def test_generated_html_has_no_conflict_markers(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")

    assert "<<<<<<<" not in html
    assert "=======" not in html
    assert ">>>>>>>" not in html


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


def test_missing_industry_low_priority_is_collapsed_outside_overview(tmp_path: Path) -> None:
    _write_reports(tmp_path)
    pd.DataFrame(
        [
            {
                "stock_id": "1002",
                "stock_name": "低優先測試",
                "market_type": "ETF",
                "latest_relative_mode": "market_relative_fallback",
                "fallback_reason": "缺少產業分類，使用全市場相對強弱",
                "recent_appearance_count": 0,
                "priority_score": 1,
                "priority_level": "LOW",
                "suggested_action": "暫緩補資料",
            },
        ]
    ).to_csv(tmp_path / "missing_industry_priority.csv", index=False, encoding="utf-8")

    output_path = generate_html_report(tmp_path)
    html = output_path.read_text(encoding="utf-8")
    overview_start = html.index('id="dashboard-overview"')
    overview_end = html.index("<section", overview_start + 1)
    overview = html[overview_start:overview_end]

    assert "高優先缺口已清空" in overview
    assert "<b>HIGH</b>" in overview
    assert "<b>MEDIUM</b>" in overview
    assert "LOW" not in overview
    assert 'class="kpi-card ok"><h3>產業分類缺口</h3>' in overview

    missing_start = html.index('id="missing-industry-section"')
    missing_end = html.index("<section", missing_start + 1)
    missing_section = html[missing_start:missing_end]
    assert "LOW priority 低優先缺口（1 筆，收合資訊）" in missing_section
    assert '<details class="collapse-block missing-low-priority-details">' in missing_section
    assert 'badge-warning">低' not in missing_section


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
