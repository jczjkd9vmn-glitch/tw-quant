from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.generate_html_report import generate_html_report, _today_exit_frame


def test_fallback_note_is_consistent_when_requested_equals_fallback(tmp_path: Path) -> None:
    _write_reports(
        tmp_path,
        summary_overrides={
            "requested_date": "2026-05-08",
            "trade_date": "2026-05-08",
            "fallback_date": "2026-05-08",
            "fallback_reason": "no trading data",
            "market_intel_warning_count": 0,
        },
        data_fetch_status=_status_frame([("institutional", "OK", 10, "best_effort", "wrote_new_data")]),
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "本次使用原始交易日資料，未切換至替代交易日。" in html
    assert "今日無交易資料，已使用最近有效交易日" not in html
    assert "是否使用最近有效資料" in html
    assert "否" in html


def test_fallback_note_only_shows_recent_data_when_dates_differ(tmp_path: Path) -> None:
    _write_reports(
        tmp_path,
        summary_overrides={
            "requested_date": "2026-05-10",
            "trade_date": "2026-05-08",
            "fallback_date": "2026-05-08",
            "fallback_reason": "no trading data",
        },
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "今日無交易資料，已使用最近有效交易日" in html
    assert "本次使用原始交易日資料，未切換至替代交易日。" not in html


def test_key_conclusion_reports_market_intel_warnings_and_fetch_status(tmp_path: Path) -> None:
    _write_reports(tmp_path, summary_overrides={"market_intel_warning_count": 2})
    html = generate_html_report(tmp_path).read_text(encoding="utf-8")
    assert "市場情報資料不足，未影響流程" in html
    assert "無重大錯誤" not in html

    cache_dir = tmp_path / "cache_case"
    _write_reports(
        cache_dir,
        summary_overrides={"market_intel_warning_count": 0, "market_intel_status": "CACHE"},
        data_fetch_status=_status_frame([("institutional", "OK", 1, "best_effort", "wrote_new_data")]),
    )
    cache_html = generate_html_report(cache_dir).read_text(encoding="utf-8")
    assert "市場情報使用快取資料" in cache_html

    failed_dir = tmp_path / "failed_case"
    _write_reports(
        failed_dir,
        summary_overrides={"market_intel_warning_count": 0, "market_intel_status": "OK"},
        data_fetch_status=_status_frame([("monthly_revenue", "FAILED", 0, "best_effort", "kept_existing_csv")]),
    )
    failed_html = generate_html_report(failed_dir).read_text(encoding="utf-8")
    assert "月營收資料尚未取得，已保留既有資料，不影響今日流程" in failed_html


def test_key_conclusion_uses_recent_trading_day_labels_when_fallback_active(tmp_path: Path) -> None:
    _write_reports(
        tmp_path,
        summary_overrides={
            "requested_date": "2026-05-16",
            "trade_date": "2026-05-15",
            "fallback_date": "2026-05-15",
            "market_intel_warning_count": 0,
        },
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "最近有效交易日候選股數量" in html
    assert "資料交易日" in html
    assert "最近有效交易日停利筆數" in html
    assert "今日候選股數量" not in html


def test_key_conclusion_uses_today_labels_when_no_fallback(tmp_path: Path) -> None:
    _write_reports(
        tmp_path,
        summary_overrides={
            "requested_date": "2026-05-15",
            "trade_date": "2026-05-15",
            "fallback_date": "2026-05-15",
            "market_intel_warning_count": 0,
        },
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "今日候選股數量" in html
    assert "最近有效交易日候選股數量" not in html


def test_data_quality_summary_collects_multiple_issues_without_raw_urls(tmp_path: Path) -> None:
    _write_reports(
        tmp_path,
        summary_overrides={"market_intel_warning_count": 2, "market_intel_status": "CACHE"},
        data_fetch_status=pd.DataFrame(
            [
                {
                    "source_name": "monthly_revenue",
                    "provider_maturity": "best_effort",
                    "status": "OK_WITH_FALLBACK",
                    "rows": 10,
                    "warning": "official source returned no data; provider empty, kept existing csv",
                    "error_message": "HTTPError: 404 Client Error: for url: https://mops.twse.com.tw/nas/t21/sii/t21sc03_115_5_0.html",
                    "fallback_action": "kept_existing_csv",
                },
                {
                    "source_name": "valuation",
                    "provider_maturity": "csv_fallback",
                    "status": "EMPTY",
                    "rows": 0,
                    "warning": "empty",
                    "error_message": "",
                    "fallback_action": "wrote_empty_schema",
                },
            ]
        ),
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "市場情報資料不足，未影響流程" in html
    assert "市場情報使用快取資料" in html
    assert "月營收資料尚未取得，已保留既有資料，不影響今日流程" in html
    assert "部分資料來源為空，採中性或既有資料" in html
    assert "https://mops.twse.com.tw" not in html.split("資料品質摘要", 1)[1].split("</section>", 1)[0]


def test_data_quality_summary_only_says_no_issue_when_clean(tmp_path: Path) -> None:
    _write_reports(
        tmp_path,
        summary_overrides={"market_intel_warning_count": 0, "market_intel_status": "OK"},
        data_fetch_status=_status_frame([("institutional", "OK", 10, "best_effort", "wrote_new_data")]),
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "無重大錯誤" in html


def test_health_checks_include_data_fetch_status_without_mojibake_status(tmp_path: Path) -> None:
    _write_reports(
        tmp_path,
        summary_overrides={"market_intel_warning_count": 0},
        data_fetch_status=_status_frame(
            [
                ("monthly_revenue", "FAILED", 0, "best_effort", "kept_existing_csv"),
                ("margin_short", "EMPTY", 0, "best_effort", "wrote_empty_schema"),
                ("institutional", "CACHE", 3, "best_effort", "cache_used"),
                ("material_events", "OK_WITH_FALLBACK", 5, "placeholder", "kept_existing_csv"),
            ]
        ),
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "資料來源：monthly_revenue" in html
    assert "資料來源：margin_short" in html
    assert "資料來源：institutional" in html
    assert "正常資料源數" in html
    assert "注意資料源數" in html
    assert "警告資料源數" in html
    assert "失敗" in html
    assert "無資料" in html
    assert "使用快取資料" in html
    assert "成功，保留既有資料" in html
    assert "霅血" not in html
    assert "瘜冽" not in html
    assert "甇" not in html


def test_monthly_revenue_404_top_warning_is_human_readable(tmp_path: Path) -> None:
    _write_reports(
        tmp_path,
        summary_overrides={"market_intel_warning_count": 0},
        data_fetch_status=pd.DataFrame(
            [
                {
                    "source_name": "monthly_revenue",
                    "provider_maturity": "best_effort",
                    "status": "OK_WITH_FALLBACK",
                    "rows": 100,
                    "warning": "official source returned no data; provider empty, kept existing csv",
                    "error_message": "HTTPError: 404 Client Error: for url: https://mops.twse.com.tw/nas/t21/sii/t21sc03_115_5_0.html",
                    "fallback_action": "kept_existing_csv",
                }
            ]
        ),
    )

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")
    top_warning = html.split('class="top-warning"', 1)[1].split("</div>", 1)[0]

    assert "月營收資料尚未取得，已保留既有資料，不影響今日流程" in top_warning
    assert "https://mops.twse.com.tw" not in top_warning
    assert "HTTPError: 404 Client Error" in html


def test_market_intel_page_discloses_mock_and_score_usage(tmp_path: Path) -> None:
    _write_reports(tmp_path, summary_overrides={"market_intel_warning_count": 2})

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "市場情報 / 多因子" in html
    assert "資料可信度總覽" in html
    assert "基本面資料不足股票數" in html
    assert "目前基本面資料完整度不足，多數股票使用中性分數 50，請勿視為完整財報分析。" in html
    assert "total_score 是技術面原始候選分數" in html
    assert "final_market_score 是市場情報綜合分，目前不直接影響下單" in html
    assert "目前為 mock / 中性資料" in html
    assert "尚未接入正式新聞來源" in html
    assert "最新新聞標題" not in html
    assert "新聞偏負面候選" not in html


def test_attention_and_disposition_are_disclosed(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "注意股" in html
    assert "注意股：列為注意股票" in html
    assert "注意股，短線波動風險偏高，預設不阻擋但需人工確認" in html
    assert "處置股" in html
    assert "處置股：分盤交易" in html
    assert "處置股，預設阻擋新增進場" in html
    assert "無重大事件資料" not in html


def test_legacy_open_position_display_uses_fallback_text(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "2026-05-08（舊資料 fallback）" in html
    assert "成交價格來源</dt><dd>舊資料未記錄</dd>" in html
    assert "買進手續費</dt><dd>舊資料未記錄</dd>" in html
    assert "累計成本</dt><dd>舊資料未記錄</dd>" in html
    assert "最近部分出場原因" in html
    assert "最近部分出場日期" in html
    assert "出場原因" in html


def test_exit_strategy_open_position_table_uses_partial_exit_label(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")
    section = html.split("出場策略持倉明細", 1)[1].split("</details>", 1)[0]

    assert "最近部分出場原因" in section
    assert "<th>出場原因</th>" not in section


def test_today_exit_detail_filters_by_trade_date(tmp_path: Path) -> None:
    trades = _paper_trades()
    closed = trades[trades["status"] == "CLOSED"]
    open_positions = trades[trades["status"] == "OPEN"]

    today = _today_exit_frame(closed, open_positions, "2026-05-08")

    assert set(today["stock_id"]) == {"2330", "9999"}
    assert "8888" not in set(today["stock_id"])
    assert "部分停利 / 部分出場" in set(today["exit_type"])
    assert "完整出場" in set(today["exit_type"])
    _write_reports(tmp_path)
    html = generate_html_report(tmp_path).read_text(encoding="utf-8")
    assert "今日出場明細" in html
    assert "累計已平倉交易明細" in html
    assert "部分停利 / 部分出場" in html
    assert "部分出場紀錄" in html


def test_pending_orders_are_split_between_waiting_and_skipped(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "<span>等待進場</span><strong>1</strong>" in html
    assert "<span>已略過</span><strong>1</strong>" in html
    assert "已有持倉，略過重複進場" in html


def test_open_position_can_enrich_from_market_intel_when_not_candidate(tmp_path: Path) -> None:
    _write_reports(tmp_path)
    _candidates().iloc[[1]].to_csv(tmp_path / "candidates_20260508.csv", index=False, encoding="utf-8-sig")
    _candidates().iloc[[1]].to_csv(tmp_path / "risk_pass_candidates_20260508.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "測試一",
                "final_market_score": 66.0,
                "confidence_score": 77.0,
                "risk_flags": "市場情報補值",
                "final_comment": "由 market_intel 補上",
                "market_intel_source": "official",
            }
        ]
    ).to_csv(tmp_path / "market_intel_20260508.csv", index=False, encoding="utf-8-sig")

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "66.00" in html
    assert "77.00" in html
    assert "由 market_intel 補上" in html


def test_open_position_without_any_market_context_shows_missing_message(tmp_path: Path) -> None:
    _write_reports(tmp_path)
    _candidates().iloc[[1]].to_csv(tmp_path / "candidates_20260508.csv", index=False, encoding="utf-8-sig")
    _candidates().iloc[[1]].to_csv(tmp_path / "risk_pass_candidates_20260508.csv", index=False, encoding="utf-8-sig")

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "今日未入選候選股，暫無最新多因子資料" in html


def test_report_wording_and_css_are_updated(tmp_path: Path) -> None:
    _write_reports(tmp_path)

    html = generate_html_report(tmp_path).read_text(encoding="utf-8")

    assert "帳戶總資產" in html
    assert "目前持倉投入成本" in html
    assert "相對初始資金損益" in html
    assert "市場情報資料不足股票數" in html
    assert "總現值" not in html
    assert ".mobile-cards" in html
    assert html.count(".health.正常 strong") == 1


def _write_reports(
    path: Path,
    *,
    summary_overrides: dict[str, object] | None = None,
    data_fetch_status: pd.DataFrame | None = None,
) -> None:
    path.mkdir(parents=True, exist_ok=True)
    summary = {
        "requested_date": "2026-05-10",
        "trade_date": "2026-05-08",
        "fallback_date": "2026-05-08",
        "fallback_reason": "no trading data",
        "scored_rows": 3,
        "candidate_rows": 2,
        "risk_pass_rows": 1,
        "pending_orders": 1,
        "executed_orders": 0,
        "skipped_orders": 1,
        "open_positions": 1,
        "closed_positions": 2,
        "unrealized_pnl": 1000.0,
        "realized_pnl": 500.0,
        "total_equity": 1_001_500.0,
        "total_equity_after_cost": 1_001_400.0,
        "total_cost": 100.0,
        "market_intel_status": "OK",
        "market_intel_warning_count": 1,
        "market_intel_top_score": 60.0,
        "status": "OK_WITH_FALLBACK",
        "error_message": "",
    }
    summary.update(summary_overrides or {})
    pd.DataFrame([summary]).to_csv(path / "daily_summary_20260510.csv", index=False, encoding="utf-8-sig")

    candidates = _candidates()
    candidates.to_csv(path / "candidates_20260508.csv", index=False, encoding="utf-8-sig")
    candidates.head(1).to_csv(path / "risk_pass_candidates_20260508.csv", index=False, encoding="utf-8-sig")
    _pending_orders().to_csv(path / "pending_orders_20260508.csv", index=False, encoding="utf-8-sig")
    _paper_trades().to_csv(path / "paper_trades.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "total_capital": 1_000_000.0,
                "invested_value": 100_000.0,
                "market_value": 101_000.0,
                "cash": 900_000.0,
                "unrealized_pnl": 1000.0,
                "realized_pnl": 500.0,
                "total_equity": 1_001_500.0,
                "total_cost": 100.0,
                "realized_pnl_after_cost": 400.0,
                "total_equity_after_cost": 1_001_400.0,
            }
        ]
    ).to_csv(path / "paper_summary_20260508.csv", index=False, encoding="utf-8-sig")
    (data_fetch_status if data_fetch_status is not None else _status_frame([("institutional", "OK", 2, "best_effort", "wrote_new_data")])).to_csv(
        path / "data_fetch_status_20260508.csv",
        index=False,
        encoding="utf-8-sig",
    )


def _candidates() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "rank": 1,
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "測試一",
                "close": 100.0,
                "total_score": 80.0,
                "multi_factor_score": 50.0,
                "fundamental_score": 50.0,
                "fundamental_reason": "基本面資料不足，採中性分數",
                "valuation_score": 50.0,
                "valuation_reason": "資料不足",
                "financial_score": 50.0,
                "financial_reason": "資料不足",
                "market_intel_source": "mock",
                "market_intel_warning": "資料不足",
                "final_market_score": 50.0,
                "confidence_score": 45.0,
                "risk_flags": "",
                "system_comment": "",
                "event_reason": "無重大事件資料",
                "is_attention_stock": True,
                "attention_reason": "列為注意股票",
                "is_disposition_stock": False,
                "disposition_reason": "",
                "event_blocked": False,
                "risk_pass": 1,
            },
            {
                "rank": 2,
                "trade_date": "2026-05-08",
                "stock_id": "2317",
                "stock_name": "測試二",
                "close": 80.0,
                "total_score": 70.0,
                "multi_factor_score": 50.0,
                "fundamental_score": 50.0,
                "fundamental_reason": "基本面資料不足，採中性分數",
                "valuation_score": 50.0,
                "valuation_reason": "資料不足",
                "financial_score": 50.0,
                "financial_reason": "資料不足",
                "market_intel_source": "mock",
                "market_intel_warning": "資料不足",
                "final_market_score": 50.0,
                "confidence_score": 45.0,
                "risk_flags": "",
                "system_comment": "",
                "event_reason": "無重大事件資料",
                "is_attention_stock": False,
                "attention_reason": "",
                "is_disposition_stock": True,
                "disposition_reason": "分盤交易",
                "event_blocked": True,
                "risk_pass": 0,
            },
        ]
    )


def _pending_orders() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"signal_date": "2026-05-08", "stock_id": "2330", "stock_name": "測試一", "status": "PENDING", "signal_close": 100.0},
            {
                "signal_date": "2026-05-08",
                "stock_id": "2317",
                "stock_name": "測試二",
                "status": "SKIPPED_EXISTING_POSITION",
                "skipped_reason": "已有持倉",
            },
        ]
    )


def _paper_trades() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "測試一",
                "entry_price": 100.0,
                "shares": 1000,
                "remaining_shares": 500,
                "actual_entry_date": "",
                "entry_price_source": "",
                "buy_commission": 0,
                "total_cost": 0,
                "status": "OPEN",
                "current_price": 110.0,
                "market_value": 55_000.0,
                "unrealized_pnl": 5_000.0,
                "unrealized_pnl_pct": 0.10,
                "exit_reason": "take_profit_1",
                "exit_date": "2026-05-08",
            },
            {
                "trade_date": "2026-05-06",
                "stock_id": "9999",
                "stock_name": "今日出場",
                "entry_price": 100.0,
                "remaining_shares": 0,
                "status": "CLOSED",
                "exit_date": "2026-05-08",
                "exit_reason": "STOP_LOSS",
                "exit_price": 90.0,
                "realized_pnl_after_cost": -1000.0,
                "realized_pnl_pct_after_cost": -0.10,
                "total_cost": 100.0,
            },
            {
                "trade_date": "2026-05-01",
                "stock_id": "8888",
                "stock_name": "舊日出場",
                "entry_price": 100.0,
                "remaining_shares": 0,
                "status": "CLOSED",
                "exit_date": "2026-05-07",
                "exit_reason": "TAKE_PROFIT_2",
                "exit_price": 120.0,
                "realized_pnl_after_cost": 1000.0,
                "realized_pnl_pct_after_cost": 0.20,
                "total_cost": 100.0,
            },
        ]
    )


def _status_frame(rows: list[tuple[str, str, int, str, str]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "source_name": source,
                "provider_maturity": maturity,
                "status": status,
                "rows": row_count,
                "warning": "provider failed" if status in {"FAILED", "EMPTY"} else "",
                "error_message": "provider failed" if status == "FAILED" else "",
                "fallback_action": fallback_action,
            }
            for source, status, row_count, maturity, fallback_action in rows
        ]
    )
