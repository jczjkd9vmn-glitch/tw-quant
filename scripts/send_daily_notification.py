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
    "OK_WITH_WARNING": "成功但有資料警告",
    "CACHE": "使用快取資料",
    "MISSING": "資料缺失",
    "EMPTY": "無資料",
    "DISABLED": "已停用",
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

    message = build_notification_message(
        summary.iloc[0].to_dict(),
        pages_url=pages_url,
        reports_dir=Path(reports_dir),
    )
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
            f"多因子資料更新狀態：{_format_text(summary.get('multi_factor_data_status'))}",
            f"高風險事件數：{_format_int(summary.get('high_risk_event_candidates'))}",
            f"估值警告候選股數：{_format_int(summary.get('valuation_warning_candidates'))}",
            f"財報警告候選股數：{_format_int(summary.get('financial_warning_candidates'))}",
            f"籌碼加分候選股數：{_format_int(summary.get('institutional_positive_candidates'))}",
            f"GitHub Pages 報表網址：{pages or '尚未設定'}",
        ]
    )
    return "\n".join(lines)


def build_notification_message(
    summary: dict[str, object],
    pages_url: str | None = None,
    reports_dir: str | Path | None = None,
) -> str:
    report_dir = Path(reports_dir) if reports_dir is not None else ROOT / "reports"
    requested_date = _date_text(summary.get("requested_date") or summary.get("trade_date"))
    trade_date = _date_text(summary.get("trade_date"))
    fallback_date = _date_text(summary.get("fallback_date"))
    fallback_reason = _fallback_reason_text(summary.get("fallback_reason"))
    use_recent_data = _uses_recent_data(requested_date, trade_date, fallback_date)
    trading_cost = load_config(ROOT / "config.yaml").get("trading_cost", {})
    pages = pages_url or os.getenv("GITHUB_PAGES_URL") or _infer_pages_url()
    candidates = _load_latest_report(report_dir, "candidates_*.csv")
    paper_trades = _load_report(report_dir / "paper_trades.csv")

    lines = [
        "台股紙上交易每日摘要",
        f"執行狀態：{_status_text(summary.get('status'))}",
        f"原始執行日期：{requested_date}",
        f"實際交易日：{trade_date}",
        f"使用最近有效資料：{'是' if use_recent_data else '否'}",
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
            f"目前持倉數：{_format_int(summary.get('open_positions'))}",
            f"未實現損益：{_format_signed(summary.get('unrealized_pnl'))}",
            f"累計已實現損益：{_format_signed(summary.get('realized_pnl'))}",
            f"總資產：{_format_amount(summary.get('total_equity'))}",
            f"累計交易成本：{_format_amount(summary.get('total_cost'))}",
            f"滑價假設：{_format_rate_percent(trading_cost.get('slippage_rate'))}",
            f"扣成本後總資產：{_format_amount(summary.get('total_equity_after_cost'))}",
            f"今日扣成本後已實現損益：{_format_signed(summary.get('realized_pnl_after_cost_today'))}",
            f"今日停利筆數：{_format_int(summary.get('take_profit_exits'))}",
            f"今日停損筆數：{_format_int(summary.get('stop_loss_exits'))}",
            f"今日移動停利筆數：{_format_int(summary.get('trailing_stop_exits'))}",
            f"今日趨勢出場筆數：{_format_int(summary.get('trend_exit_exits'))}",
            f"市場判斷狀態：{_status_text(summary.get('market_intel_status'))}",
            f"市場判斷警告數：{_format_int(summary.get('market_intel_warning_count'))}",
            f"多因子資料狀態：{_format_text(summary.get('multi_factor_data_status'))}",
            f"高風險事件數：{_format_int(summary.get('high_risk_event_candidates'))}",
            f"基本面加分候選股數：{_format_int(summary.get('fundamental_positive_candidates'))}",
            f"估值警告候選股數：{_format_int(summary.get('valuation_warning_candidates'))}",
            f"財報警告候選股數：{_format_int(summary.get('financial_warning_candidates'))}",
            f"籌碼加分候選股數：{_format_int(summary.get('institutional_positive_candidates'))}",
        ]
    )
    lines.extend(_candidate_digest(candidates))
    lines.extend(_risk_digest(candidates))
    lines.extend(_position_digest(paper_trades))
    lines.append(f"系統健康狀態：{_health_text(summary, candidates)}")
    lines.append(f"GitHub Pages 報表網址：{pages or '尚未設定'}")
    return "\n".join(lines)[:1900]


def _send_discord_message(
    webhook_url: str,
    message: str,
    post_func: Callable[..., object] | None = None,
) -> None:
    post = post_func or requests.post
    response = post(webhook_url, json={"content": message}, timeout=15)
    if hasattr(response, "raise_for_status"):
        response.raise_for_status()


def _candidate_digest(candidates: pd.DataFrame) -> list[str]:
    if candidates.empty:
        return ["市場判斷摘要：今日無候選股明細"]
    frame = candidates.copy()
    if "final_market_score" in frame.columns:
        frame["_score"] = pd.to_numeric(frame["final_market_score"], errors="coerce").fillna(-1)
        top = frame.sort_values("_score", ascending=False).head(5)
    else:
        top = frame.head(5)
    rows = []
    for _, row in top.iterrows():
        rows.append(
            f"{_format_text(row.get('stock_id'))} {_format_text(row.get('stock_name'))} "
            f"{_format_amount(row.get('final_market_score'))}分 "
            f"{_format_text(row.get('final_comment'))}"
        )
    return ["市場判斷摘要：", "分數最高前 5 名候選股：" + "；".join(rows)]


def _risk_digest(candidates: pd.DataFrame) -> list[str]:
    if candidates.empty or "news_sentiment_score" not in candidates.columns:
        return ["新聞風險最高前 5 名：今日無新聞風險資料"]
    frame = candidates.copy()
    frame["_news"] = pd.to_numeric(frame["news_sentiment_score"], errors="coerce").fillna(0)
    risk = frame.sort_values("_news", ascending=True).head(5)
    rows = []
    for _, row in risk.iterrows():
        rows.append(
            f"{_format_text(row.get('stock_id'))} {_format_text(row.get('stock_name'))} "
            f"新聞分數 {_format_signed(row.get('news_sentiment_score'))} "
            f"{_format_text(row.get('risk_flags'))}"
        )
    warnings = _count_non_empty(candidates, "market_intel_warning")
    return [f"新聞風險最高前 5 名：{'；'.join(rows)}", f"資料不足警告：{warnings}"]


def _position_digest(paper_trades: pd.DataFrame) -> list[str]:
    if paper_trades.empty:
        return ["目前持倉重點：目前尚無紙上交易紀錄", "今日 exit signal：無"]
    status = paper_trades["status"] if "status" in paper_trades.columns else pd.Series([""] * len(paper_trades))
    open_frame = paper_trades[status.fillna("").astype(str).str.upper() == "OPEN"].head(5)
    open_rows = [
        f"{_format_text(row.get('stock_id'))} {_format_text(row.get('stock_name'))} "
        f"未實現 {_format_signed(row.get('unrealized_pnl'))}"
        for _, row in open_frame.iterrows()
    ]
    exit_dates = paper_trades["exit_date"] if "exit_date" in paper_trades.columns else pd.Series([""] * len(paper_trades))
    exit_frame = paper_trades[exit_dates.fillna("").astype(str).str.strip() != ""].tail(5)
    exit_rows = [
        f"{_format_text(row.get('stock_id'))} {_format_text(row.get('exit_reason'))} "
        f"{_format_signed(row.get('last_exit_realized_pnl_after_cost'))}"
        for _, row in exit_frame.iterrows()
    ]
    return [
        "目前持倉重點：" + ("；".join(open_rows) if open_rows else "目前無 OPEN 持倉"),
        "今日 exit signal / 出場原因摘要：" + ("；".join(exit_rows) if exit_rows else "無"),
    ]


def _health_text(summary: dict[str, object], candidates: pd.DataFrame) -> str:
    if str(summary.get("status", "")).upper() == "FAILED":
        return f"警告：{_format_text(summary.get('error_step'))} {_format_text(summary.get('error_message'))}"
    if candidates.empty:
        return "注意：今日無候選股明細"
    if _count_non_empty(candidates, "market_intel_warning") > 0:
        return "注意：部分市場判斷資料不足"
    return "正常"


def _load_latest_report(reports_dir: Path, pattern: str) -> pd.DataFrame:
    files = sorted(reports_dir.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    if not files:
        return pd.DataFrame()
    return _load_report(files[0])


def _load_report(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _count_non_empty(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int((frame[column].fillna("").astype(str).str.strip() != "").sum())


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


def _format_text(value: object) -> str:
    if _is_blank(value):
        return "-"
    return str(value)


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
