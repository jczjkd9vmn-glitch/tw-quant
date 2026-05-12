from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import sys
from typing import Callable, Iterable

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tw_quant.config import load_config


STATUS_LABELS = {
    "OK": "成功",
    "OK_WITH_FALLBACK": "成功，使用最近有效交易日",
    "FAILED": "失敗",
    "no trading data": "無交易資料",
}


def send_daily_notification(
    reports_dir: str | Path = ROOT / "reports",
    webhook_url: str | None = None,
    pages_url: str | None = None,
    post_func: Callable[..., object] | None = None,
) -> bool:
    webhook = (webhook_url or os.getenv("DISCORD_WEBHOOK_URL", "")).strip()
    if not webhook:
        print("warning: 未設定 DISCORD_WEBHOOK_URL，略過每日通知。")
        return False

    summary = _load_latest_summary(Path(reports_dir))
    if summary.empty:
        print("warning: 找不到 daily_summary_*.csv，略過每日通知。")
        return False

    message = build_notification_message(summary.iloc[0].to_dict(), pages_url=pages_url)
    _send_discord_message(webhook, message, post_func=post_func)
    print("daily_notification=sent")
    return True


def build_notification_message(summary: dict[str, object], pages_url: str | None = None) -> str:
    requested_date = _date_text(summary.get("requested_date") or summary.get("trade_date"))
    trade_date = _date_text(summary.get("trade_date"))
    fallback_date = _date_text(summary.get("fallback_date"))
    fallback_reason = _fallback_reason_text(summary.get("fallback_reason"))
    use_recent_data = _uses_recent_data(requested_date, trade_date, fallback_date)
    trading_cost = load_config(ROOT / "config.yaml").get("trading_cost", {})
    pages = pages_url or os.getenv("GITHUB_PAGES_URL") or _infer_pages_url()

    lines = [
        "台股紙上交易每日摘要",
        f"執行狀態：{_status_text(summary.get('status'))}",
        f"原始執行日期：{requested_date}",
        f"實際交易日：{trade_date}",
        f"是否使用最近有效資料：{'是' if use_recent_data else '否'}",
    ]
    if use_recent_data:
        lines.extend(
            [
                f"使用資料日期：{trade_date if trade_date != '-' else fallback_date}",
                f"原因：{fallback_reason}",
            ]
        )

    lines.extend(
        [
            f"候選股數：{_format_int(summary.get('candidate_rows'))}",
            f"通過風控數：{_format_int(summary.get('risk_pass_rows'))}",
            f"待進場筆數：{_format_int(summary.get('pending_orders'))}",
            f"今日成交筆數：{_format_int(summary.get('executed_orders'))}",
            f"跳過進場筆數：{_format_int(summary.get('skipped_orders'))}",
            f"新增持倉數：{_format_int(summary.get('new_positions'))}",
            f"目前持倉數：{_format_int(summary.get('open_positions'))}",
            f"未實現損益：{_format_signed(summary.get('unrealized_pnl'))}",
            f"累計已實現損益：{_format_signed(summary.get('realized_pnl'))}",
            f"總資產：{_format_amount(summary.get('total_equity'))}",
            f"累計交易成本：{_format_amount(summary.get('total_cost'))}",
            f"滑價假設：{_format_rate_percent(trading_cost.get('slippage_rate'))}",
            f"累計扣成本後已實現損益：{_format_signed(summary.get('realized_pnl_after_cost'))}",
            f"扣成本後總資產：{_format_amount(summary.get('total_equity_after_cost'))}",
            f"今日停利筆數：{_format_int(summary.get('take_profit_exits'))}",
            f"今日停損筆數：{_format_int(summary.get('stop_loss_exits'))}",
            f"今日移動停利筆數：{_format_int(summary.get('trailing_stop_exits'))}",
            f"今日趨勢出場筆數：{_format_int(summary.get('trend_exit_exits'))}",
            f"今日扣成本後已實現損益：{_format_signed(summary.get('realized_pnl_after_cost_today'))}",
            f"今日基本面加分候選股數：{_format_int(summary.get('fundamental_positive_candidates'))}",
            f"今日基本面警告候選股數：{_format_int(summary.get('fundamental_warning_candidates'))}",
            f"GitHub Pages 報表網址：{pages or '尚未設定'}",
        ]
    )
    return "\n".join(lines)


def _send_discord_message(
    webhook_url: str,
    message: str,
    post_func: Callable[..., object] | None = None,
) -> None:
    post = post_func or requests.post
    response = post(webhook_url, json={"content": message}, timeout=15)
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()


def _load_latest_summary(reports_dir: Path) -> pd.DataFrame:
    latest = _latest_summary_file(reports_dir)
    if latest is None:
        return pd.DataFrame()
    return pd.read_csv(latest, encoding="utf-8-sig")


def _latest_summary_file(reports_dir: Path) -> Path | None:
    files = list(reports_dir.glob("daily_summary_*.csv"))
    if not files:
        return None
    return sorted(files, key=lambda path: (_date_from_filename(path) or pd.Timestamp.min), reverse=True)[0]


def _date_from_filename(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed


def _infer_pages_url() -> str:
    repository = os.getenv("GITHUB_REPOSITORY", "").strip()
    if "/" not in repository:
        return ""
    owner, repo = repository.split("/", 1)
    return f"https://{owner}.github.io/{repo}/"


def _status_text(value: object) -> str:
    if _is_blank(value):
        return "-"
    text = str(value).strip()
    return STATUS_LABELS.get(text, text)


def _fallback_reason_text(value: object) -> str:
    if _is_blank(value):
        return "-"
    text = str(value).strip()
    if text == "no trading data":
        return "本次無新交易資料，使用資料庫最近有效資料"
    return STATUS_LABELS.get(text, text)


def _uses_recent_data(requested_date: str, trade_date: str, fallback_date: str) -> bool:
    if requested_date != "-" and trade_date != "-":
        return requested_date != trade_date
    return fallback_date != "-"


def _date_text(value: object) -> str:
    if _is_blank(value):
        return "-"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return "-"
    return parsed.strftime("%Y-%m-%d")


def _format_int(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number:,.0f}"


def _format_amount(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number:,.0f}"


def _format_signed(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    if number > 0:
        return f"+{number:,.0f}"
    if number < 0:
        return f"{number:,.0f}"
    return "0"


def _format_rate_percent(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    text = f"{number * 100:.3f}".rstrip("0").rstrip(".")
    return f"{text}%"


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="發送每日紙上交易摘要通知。")
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--pages-url", default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        send_daily_notification(reports_dir=args.reports_dir, pages_url=args.pages_url)
    except requests.RequestException as exc:
        print(f"warning: Discord 通知發送失敗：{exc}")


if __name__ == "__main__":
    main()
