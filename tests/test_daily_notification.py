from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.send_daily_notification import build_notification_message, send_daily_notification


def test_send_daily_notification_warns_without_webhook(tmp_path: Path, monkeypatch, capsys) -> None:
    _write_summary(tmp_path)
    monkeypatch.delenv("DISCORD_WEBHOOK_URL", raising=False)

    sent = send_daily_notification(reports_dir=tmp_path)

    captured = capsys.readouterr()
    assert sent is False
    assert "未設定 DISCORD_WEBHOOK_URL" in captured.out


def test_build_notification_message_uses_traditional_chinese_and_fallback_url(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/tw-quant")
    summary = _summary_row()

    message = build_notification_message(summary)

    assert "台股紙上交易每日摘要" in message
    assert "執行狀態：成功，使用最近有效交易日" in message
    assert "原始執行日期：2026-05-10" in message
    assert "實際交易日：2026-05-08" in message
    assert "是否使用最近有效資料：是" in message
    assert "使用資料日期：2026-05-08" in message
    assert "原因：本次無新交易資料，使用資料庫最近有效資料" in message
    assert "替代交易日" not in message
    assert "候選股數：20" in message
    assert "通過風控數：6" in message
    assert "待進場筆數：4" in message
    assert "今日成交筆數：2" in message
    assert "跳過進場筆數：1" in message
    assert "新增持倉數：0" in message
    assert "目前持倉數：6" in message
    assert "未實現損益：+1,234" in message
    assert "累計已實現損益：0" in message
    assert "總資產：1,001,234" in message
    assert "累計交易成本：123" in message
    assert "滑價假設：0.1%" in message
    assert "累計扣成本後已實現損益：-123" in message
    assert "扣成本後總資產：1,001,111" in message
    assert "今日停利筆數：1" in message
    assert "今日停損筆數：0" in message
    assert "今日移動停利筆數：0" in message
    assert "今日趨勢出場筆數：0" in message
    assert "今日扣成本後已實現損益：+100" in message
    assert "今日基本面加分候選股數：2" in message
    assert "今日基本面警告候選股數：1" in message
    assert "多因子資料更新狀態：OK:5" in message
    assert "高風險事件數：1" in message
    assert "估值警告候選股數：2" in message
    assert "財報警告候選股數：3" in message
    assert "籌碼加分候選股數：4" in message
    assert "GitHub Pages 報表網址：https://owner.github.io/tw-quant/" in message


def test_build_notification_message_omits_fallback_details_when_requested_date_matches_trade_date() -> None:
    summary = {
        **_summary_row(),
        "requested_date": "2026-05-08",
        "trade_date": "2026-05-08",
        "fallback_date": "",
        "fallback_reason": "",
        "status": "OK",
    }

    message = build_notification_message(summary, pages_url="https://example.github.io/tw-quant/")

    assert "是否使用最近有效資料：否" in message
    assert "使用資料日期：" not in message
    assert "原因：" not in message
    assert "替代交易日" not in message


def test_build_notification_message_flags_stale_actual_data_date() -> None:
    summary = {
        **_summary_row(),
        "requested_date": "2026-06-13",
        "trade_date": "2026-06-13",
        "fallback_date": "",
        "fallback_reason": "",
        "status": "OK",
        "actual_data_date": "2026-06-09",
        "cache_age_days": 4,
        "trading_day_lag": 3,
        "market_closed": False,
        "is_stale_data": True,
        "used_latest_available": True,
    }

    message = build_notification_message(summary, pages_url="https://example.github.io/tw-quant/")

    assert "使用最近有效資料：是" in message
    assert "使用最近有效資料：否" not in message
    assert "使用資料日期：2026-06-09" in message
    assert "實際資料日：2026-06-09" in message
    assert "資料年齡天數：4" in message
    assert "落後有效交易日：3" in message
    assert "市場是否休市：否" in message
    assert "使用最近交易日資料：是" in message
    assert "是否過期資料：是" in message


def test_build_notification_message_reports_market_closed_recent_trading_day() -> None:
    summary = {
        **_summary_row(),
        "requested_date": "2026-06-20",
        "trade_date": "2026-06-19",
        "actual_data_date": "2026-06-19",
        "cache_age_days": 1,
        "trading_day_lag": 0,
        "market_closed": True,
        "is_stale_data": False,
        "used_latest_available": True,
        "fallback_date": "2026-06-19",
        "fallback_reason": "non_trading_day",
    }

    message = build_notification_message(summary, pages_url="https://example.github.io/tw-quant/")

    assert "原始執行日期：2026-06-20" in message
    assert "實際資料日：2026-06-19" in message
    assert "落後有效交易日：0" in message
    assert "市場是否休市：是" in message
    assert "使用最近交易日資料：是" in message
    assert "是否過期資料：否" in message


def test_build_notification_message_reports_stale_public_docs_kept(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "docs_written": False,
                "docs_publish_status": "SKIPPED_STALE",
                "docs_publish_reason": (
                    "stale data exceeded public_report.stale_days_threshold=2 "
                    "(cache_age_days=3); kept previous docs/index.html."
                ),
            }
        ]
    ).to_csv(tmp_path / "public_report_publish_status.csv", index=False, encoding="utf-8")

    message = build_notification_message(
        _summary_row(),
        reports_dir=tmp_path,
        pages_url="https://example.github.io/tw-quant/",
    )

    assert "GitHub Pages 發布狀態：資料過期，本次保留既有 public docs" in message
    assert "kept previous docs/index.html" in message


def test_send_daily_notification_posts_to_discord(tmp_path: Path) -> None:
    _write_summary(tmp_path)
    calls = []

    class Response:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    sent = send_daily_notification(
        reports_dir=tmp_path,
        webhook_url="https://discord.example/webhook",
        pages_url="https://example.github.io/tw-quant/",
        post_func=fake_post,
    )

    assert sent is True
    assert calls[0][0] == "https://discord.example/webhook"
    assert calls[0][1]["json"]["content"].startswith("台股紙上交易每日摘要")
    assert "https://example.github.io/tw-quant/" in calls[0][1]["json"]["content"]
    assert calls[0][1]["timeout"] == 15


def test_send_daily_notification_uses_latest_daily_summary(tmp_path: Path) -> None:
    _write_summary(tmp_path, date_label="20260508", candidate_rows=1)
    _write_summary(tmp_path, date_label="20260510", candidate_rows=20)
    calls = []

    class Response:
        def raise_for_status(self) -> None:
            return None

    def fake_post(_url, **kwargs):
        calls.append(kwargs["json"]["content"])
        return Response()

    send_daily_notification(
        reports_dir=tmp_path,
        webhook_url="https://discord.example/webhook",
        pages_url="https://example.github.io/tw-quant/",
        post_func=fake_post,
    )

    assert "候選股數：20" in calls[0]


def test_build_notification_message_includes_decision_summary(tmp_path: Path) -> None:
    _write_summary(tmp_path)
    pd.DataFrame(
        [
            {
                "stock_id": "2330",
                "stock_name": "台積電",
                "decision": "BUY_CANDIDATE",
                "candidate_grade": "A",
                "multi_factor_score": 80,
            },
            {
                "stock_id": "2603",
                "stock_name": "長榮",
                "decision": "NO_TRADE",
                "candidate_grade": "D",
                "liquidity_score": 20,
            },
        ]
    ).to_csv(tmp_path / "trading_decisions_20260508.csv", index=False, encoding="utf-8")

    message = build_notification_message(
        {**_summary_row(), "grade_a_count": 1, "buy_candidate_count": 1, "no_trade_count": 1},
        reports_dir=tmp_path,
    )

    assert "僅供人工確認" in message
    assert "買進候選數：1" in message
    assert "前 5 名 BUY_CANDIDATE：2330 台積電 A BUY_CANDIDATE" in message
    assert "前 5 名 HIGH_RISK / NO_TRADE：2603 長榮 D NO_TRADE" in message
    assert "保證獲利" not in message


def test_build_notification_message_handles_candidates_without_score_columns(tmp_path: Path) -> None:
    _write_summary(tmp_path)
    pd.DataFrame(
        [
            {
                "trade_date": "2026-05-08",
                "stock_id": "2330",
                "stock_name": "台積電",
            }
        ]
    ).to_csv(tmp_path / "candidates_20260508.csv", index=False, encoding="utf-8")

    message = build_notification_message(_summary_row(), reports_dir=tmp_path)

    assert "今日綜合分數最高前 5 名：2330 台積電 -分" in message


def _write_summary(path: Path, date_label: str = "20260510", candidate_rows: int = 20) -> None:
    pd.DataFrame([{**_summary_row(), "candidate_rows": candidate_rows}]).to_csv(
        path / f"daily_summary_{date_label}.csv",
        index=False,
        encoding="utf-8",
    )


def _summary_row() -> dict[str, object]:
    return {
        "requested_date": "2026-05-10",
        "trade_date": "2026-05-08",
        "fallback_date": "2026-05-08",
        "fallback_reason": "no trading data",
        "candidate_rows": 20,
        "risk_pass_rows": 6,
        "pending_orders": 4,
        "executed_orders": 2,
        "skipped_orders": 1,
        "new_positions": 0,
        "open_positions": 6,
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
        "fundamental_positive_candidates": 2,
        "fundamental_warning_candidates": 1,
        "multi_factor_data_status": "OK:5",
        "high_risk_event_candidates": 1,
        "valuation_warning_candidates": 2,
        "financial_warning_candidates": 3,
        "institutional_positive_candidates": 4,
        "status": "OK_WITH_FALLBACK",
    }


def test_build_notification_message_uses_traditional_chinese_and_fallback_url(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/tw-quant")
    summary = _summary_row()

    message = build_notification_message(summary)

    assert "台股紙上交易每日摘要" in message
    assert "執行狀態：成功，使用最近有效交易日" in message
    assert "原始執行日期：2026-05-10" in message
    assert "實際交易日：2026-05-08" in message
    assert "使用最近有效資料：是" in message
    assert "使用資料日期：2026-05-08" in message
    assert "本次無新交易資料，使用資料庫最近有效資料" in message
    assert "候選股數：20" in message
    assert "通過風控數：6" in message
    assert "待進場筆數：4" in message
    assert "今日成交筆數：2" in message
    assert "跳過進場筆數：1" in message
    assert "未實現損益：+1,234" in message
    assert "累計已實現損益：0" in message
    assert "累計交易成本：123" in message
    assert "滑價假設：0.1%" in message
    assert "今日扣成本後已實現損益：+100" in message
    assert "市場判斷狀態" in message
    assert "GitHub Pages 報表網址：https://owner.github.io/tw-quant/" in message


def test_build_notification_message_omits_fallback_details_when_requested_date_matches_trade_date() -> None:
    summary = {
        **_summary_row(),
        "requested_date": "2026-05-08",
        "trade_date": "2026-05-08",
        "fallback_date": "",
        "fallback_reason": "",
        "status": "OK",
    }

    message = build_notification_message(summary, pages_url="https://example.github.io/tw-quant/")

    assert "使用最近有效資料：否" in message
    assert "使用資料日期：" not in message
    assert "原因：" not in message
    assert "替代交易日" not in message


def test_send_daily_notification_posts_to_discord(tmp_path: Path) -> None:
    _write_summary(tmp_path)
    calls = []

    class Response:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    sent = send_daily_notification(
        reports_dir=tmp_path,
        webhook_url="https://discord.example/webhook",
        pages_url="https://example.github.io/tw-quant/",
        post_func=fake_post,
    )

    assert sent is True
    assert calls[0][0] == "https://discord.example/webhook"
    assert calls[0][1]["json"]["content"].startswith("台股紙上交易每日摘要")
    assert "https://example.github.io/tw-quant/" in calls[0][1]["json"]["content"]
    assert calls[0][1]["timeout"] == 15


def test_send_daily_notification_uses_latest_daily_summary(tmp_path: Path) -> None:
    _write_summary(tmp_path, date_label="20260508", candidate_rows=1)
    _write_summary(tmp_path, date_label="20260510", candidate_rows=20)
    calls = []

    class Response:
        def raise_for_status(self) -> None:
            return None

    def fake_post(_url, **kwargs):
        calls.append(kwargs["json"]["content"])
        return Response()

    send_daily_notification(
        reports_dir=tmp_path,
        webhook_url="https://discord.example/webhook",
        pages_url="https://example.github.io/tw-quant/",
        post_func=fake_post,
    )

    assert "候選股數：20" in calls[0]
