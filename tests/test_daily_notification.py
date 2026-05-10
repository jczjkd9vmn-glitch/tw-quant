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
    assert "是否使用替代交易日：是（無交易資料，使用 2026-05-08）" in message
    assert "候選股數：20" in message
    assert "通過風控數：6" in message
    assert "待進場筆數：4" in message
    assert "今日成交筆數：2" in message
    assert "跳過進場筆數：1" in message
    assert "新增持倉數：0" in message
    assert "目前持倉數：6" in message
    assert "未實現損益：+1,234" in message
    assert "已實現損益：0" in message
    assert "總資產：1,001,234" in message
    assert "累計交易成本：123" in message
    assert "扣成本後已實現損益：-123" in message
    assert "扣成本後總資產：1,001,111" in message
    assert "GitHub Pages 報表網址：https://owner.github.io/tw-quant/" in message


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


def _write_summary(path: Path, date_label: str = "20260510", candidate_rows: int = 20) -> None:
    pd.DataFrame([{**_summary_row(), "candidate_rows": candidate_rows}]).to_csv(
        path / f"daily_summary_{date_label}.csv",
        index=False,
        encoding="utf-8-sig",
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
        "status": "OK_WITH_FALLBACK",
    }
