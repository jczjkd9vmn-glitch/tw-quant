from __future__ import annotations

import argparse
from html import escape
from pathlib import Path
import re
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


COLUMN_LABELS = {
    "rank": "排序",
    "trade_date": "實際交易日",
    "requested_date": "原始執行日期",
    "fallback_date": "使用替代交易日",
    "fallback_reason": "替代原因",
    "scored_rows": "已評分標的數",
    "candidate_rows": "候選股數",
    "risk_pass_rows": "通過風控數",
    "new_positions": "新增持倉數",
    "pending_orders": "待進場筆數",
    "executed_orders": "今日成交筆數",
    "skipped_orders": "跳過進場筆數",
    "entry_price_source_warnings": "成交價格警示數",
    "open_positions": "目前持倉數",
    "closed_positions": "已平倉數",
    "unrealized_pnl": "未實現損益",
    "realized_pnl": "已實現損益",
    "total_equity": "總資產",
    "total_cost": "累計交易成本",
    "realized_pnl_after_cost": "扣成本後已實現損益",
    "total_equity_after_cost": "扣成本後總資產",
    "total_capital": "初始資金",
    "invested_value": "投入金額",
    "market_value": "目前市值",
    "cash": "現金",
    "stock_id": "股票代號",
    "stock_name": "股票名稱",
    "close": "收盤價",
    "total_score": "總分",
    "trend_score": "趨勢分數",
    "momentum_score": "動能分數",
    "fundamental_score": "基本面分數",
    "chip_score": "籌碼分數",
    "risk_score": "風險分數",
    "is_candidate": "是否候選",
    "risk_pass": "是否通過風控",
    "risk_reason": "風控原因",
    "reason": "買進理由",
    "stop_loss_price": "停損價",
    "suggested_position_pct": "建議部位",
    "entry_price": "進場價",
    "entry_slippage": "進場滑價",
    "entry_commission": "買進手續費",
    "shares": "股數",
    "position_value": "投入金額",
    "status": "狀態",
    "signal_date": "訊號日",
    "planned_entry_date": "計畫進場日",
    "actual_entry_date": "實際進場日",
    "signal_close": "訊號日收盤價",
    "entry_price_source": "成交價格來源",
    "skipped_reason": "跳過原因",
    "warning": "警示",
    "current_price": "目前價格",
    "unrealized_pnl_pct": "未實現損益率",
    "holding_days": "持有天數",
    "stop_loss_hit": "是否觸及停損",
    "exit_date": "出場日",
    "exit_price": "出場價",
    "exit_slippage": "出場滑價",
    "exit_commission": "賣出手續費",
    "exit_tax": "交易稅",
    "realized_pnl_pct": "已實現損益率",
    "realized_pnl_pct_after_cost": "扣成本後已實現損益率",
    "exit_reason": "出場原因",
    "error_step": "失敗步驟",
    "error_message": "錯誤訊息",
}


STATUS_LABELS = {
    "OK": "成功",
    "OK_WITH_FALLBACK": "成功，使用最近有效交易日",
    "FAILED": "失敗",
    "OPEN": "持有中",
    "CLOSED": "已出場",
    "STOP_LOSS": "停損",
    "TAKE_PROFIT_1": "第一段停利",
    "TAKE_PROFIT_2": "第二段停利",
    "TRAILING_STOP": "移動停利",
    "MA_EXIT": "跌破 20 日均線",
    "TIME_EXIT": "持有過久出場",
    "PENDING": "等待進場",
    "EXECUTED": "已成交",
    "SKIPPED_EXISTING_POSITION": "已有持倉，略過重複進場",
    "OPEN": "持有中",
    "no trading data": "無交易資料",
    "True": "是",
    "False": "否",
    "true": "是",
    "false": "否",
    "1": "是",
    "0": "否",
}

ENTRY_PRICE_SOURCE_LABELS = {
    "OPEN": "開盤價",
    "CLOSE_FALLBACK": "收盤價 fallback",
}


SCORE_COLUMNS = {
    "total_score",
    "trend_score",
    "momentum_score",
    "fundamental_score",
    "chip_score",
    "risk_score",
}
PERCENT_COLUMNS = {
    "suggested_position_pct",
    "unrealized_pnl_pct",
    "realized_pnl_pct",
    "realized_pnl_pct_after_cost",
}
PRICE_COLUMNS = {
    "close",
    "stop_loss_price",
    "entry_price",
    "entry_slippage",
    "current_price",
    "exit_price",
    "exit_slippage",
}
AMOUNT_COLUMNS = {
    "total_capital",
    "invested_value",
    "market_value",
    "cash",
    "total_equity",
    "total_equity_after_cost",
    "position_value",
    "entry_commission",
    "exit_commission",
    "exit_tax",
    "total_cost",
}
PNL_COLUMNS = {"unrealized_pnl", "realized_pnl", "realized_pnl_after_cost"}
INTEGER_COLUMNS = {
    "rank",
    "scored_rows",
    "candidate_rows",
    "risk_pass_rows",
    "new_positions",
    "open_positions",
    "closed_positions",
    "pending_orders",
    "executed_orders",
    "skipped_orders",
    "entry_price_source_warnings",
    "shares",
    "holding_days",
}
STATUS_COLUMNS = {
    "status",
    "exit_reason",
    "fallback_reason",
    "is_candidate",
    "risk_pass",
    "stop_loss_hit",
}
DATE_COLUMNS = {"trade_date", "requested_date", "fallback_date", "exit_date", "signal_date", "actual_entry_date"}


def generate_html_report(
    reports_dir: str | Path = ROOT / "reports",
    docs_dir: str | Path | None = None,
) -> Path:
    report_dir = Path(reports_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    daily_summary = _read_latest_csv(report_dir, "daily_summary_*.csv")
    recent_summaries = _read_recent_summaries(report_dir)
    candidates = _read_latest_csv(report_dir, "candidates_*.csv")
    risk_pass = _read_latest_csv(report_dir, "risk_pass_candidates_*.csv")
    paper_trades = _read_csv(report_dir / "paper_trades.csv")
    paper_summary = _read_latest_csv(report_dir, "paper_summary_*.csv")
    pending_orders = _read_all_csv(report_dir, "pending_orders_*.csv")

    html = _render_page(
        daily_summary=daily_summary,
        recent_summaries=recent_summaries,
        candidates=candidates,
        risk_pass=risk_pass,
        paper_trades=paper_trades,
        paper_summary=paper_summary,
        pending_orders=pending_orders,
    )

    output_path = report_dir / "index.html"
    output_path.write_text(html, encoding="utf-8")
    if docs_dir is not None:
        docs_path = Path(docs_dir)
        docs_path.mkdir(parents=True, exist_ok=True)
        (docs_path / "index.html").write_text(html, encoding="utf-8")
    return output_path


def _render_page(
    daily_summary: pd.DataFrame,
    recent_summaries: pd.DataFrame,
    candidates: pd.DataFrame,
    risk_pass: pd.DataFrame,
    paper_trades: pd.DataFrame,
    paper_summary: pd.DataFrame,
    pending_orders: pd.DataFrame,
) -> str:
    latest_summary = _first_row(daily_summary)
    open_positions = _filter_status(paper_trades, "OPEN")
    closed_trades = _filter_status(paper_trades, "CLOSED")
    latest_paper_summary = _first_row(paper_summary)
    key_takeaway = _key_takeaway(latest_summary, latest_paper_summary)
    health_checks = _health_checks(
        latest_summary=latest_summary,
        candidates=candidates,
        risk_pass=risk_pass,
        pending_orders=pending_orders,
        paper_trades=paper_trades,
    )
    warning_banner = _warning_banner(health_checks)

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="zh-Hant">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            "<title>台股紙上交易每日報表</title>",
            f"<style>{_css()}</style>",
            "</head>",
            "<body>",
            '<main class="page">',
            "<header>",
            "<p>台股量化系統</p>",
            "<h1>台股紙上交易每日報表</h1>",
            "<div>所有內容僅供紙上模擬交易與策略檢查使用，不代表投資建議，也不保證獲利。</div>",
            "</header>",
            warning_banner,
            _section("今日重點結論", key_takeaway),
            _section("系統健康檢查", _health_check_cards(health_checks)),
            _section("系統狀態總覽", _status_overview(latest_summary)),
            _section(
                "今日候選股",
                _table(
                    candidates,
                    [
                        "rank",
                        "trade_date",
                        "stock_id",
                        "stock_name",
                        "close",
                        "total_score",
                        "trend_score",
                        "momentum_score",
                        "risk_score",
                        "reason",
                    ],
                    "目前尚無候選股資料",
                    max_rows=20,
                ),
            ),
            _section(
                "通過風控股票",
                _table(
                    risk_pass,
                    [
                        "rank",
                        "stock_id",
                        "stock_name",
                        "close",
                        "total_score",
                        "risk_reason",
                        "stop_loss_price",
                        "suggested_position_pct",
                    ],
                    "目前尚無通過風控的股票",
                    max_rows=20,
                ),
            ),
            _section(
                "待進場清單",
                _responsive_table_and_cards(
                    pending_orders,
                    [
                        "signal_date",
                        "planned_entry_date",
                        "actual_entry_date",
                        "stock_id",
                        "stock_name",
                        "signal_close",
                        "entry_price",
                        "entry_price_source",
                        "shares",
                        "position_value",
                        "status",
                        "skipped_reason",
                        "warning",
                    ],
                    "目前尚無待進場資料",
                    max_rows=50,
                ),
            ),
            _section(
                "已成交持倉",
                _responsive_table_and_cards(
                    open_positions,
                    [
                        "signal_date",
                        "trade_date",
                        "actual_entry_date",
                        "stock_id",
                        "stock_name",
                        "entry_price",
                        "entry_price_source",
                        "entry_commission",
                        "shares",
                        "market_value",
                        "unrealized_pnl",
                        "unrealized_pnl_pct",
                        "stop_loss_price",
                        "holding_days",
                        "status",
                    ],
                    "目前尚無已成交持倉",
                    max_rows=50,
                ),
            ),
            _section("出場策略摘要", _exit_strategy_overview(open_positions, closed_trades)),
            _section("紙上交易績效", _paper_performance(latest_paper_summary, closed_trades)),
            _section("交易成本摘要", _cost_overview(latest_summary, latest_paper_summary)),
            _section(
                "最近每日 summary",
                _table(
                    recent_summaries,
                    [
                        "requested_date",
                        "trade_date",
                        "status",
                        "fallback_date",
                        "fallback_reason",
                        "scored_rows",
                        "candidate_rows",
                        "risk_pass_rows",
                        "pending_orders",
                        "executed_orders",
                        "skipped_orders",
                        "entry_price_source_warnings",
                        "open_positions",
                        "closed_positions",
                        "unrealized_pnl",
                        "realized_pnl",
                        "total_equity",
                        "total_cost",
                        "realized_pnl_after_cost",
                        "total_equity_after_cost",
                    ],
                    "目前尚無每日 summary",
                    max_rows=10,
                ),
            ),
            _section("非交易日替代交易日說明", _fallback_note(latest_summary)),
            "</main>",
            "</body>",
            "</html>",
        ]
    )


def _status_overview(summary: dict[str, object]) -> str:
    if not summary:
        return _empty("目前尚無每日 summary，請先執行每日流程")

    cards = [
        ("執行狀態", _format_cell("status", summary.get("status"))),
        ("原始執行日期", _format_cell("requested_date", summary.get("requested_date") or summary.get("trade_date"))),
        ("實際交易日", _format_cell("trade_date", summary.get("trade_date"))),
        ("使用替代交易日", _format_cell("fallback_date", summary.get("fallback_date"))),
        ("已評分標的數", _format_cell("scored_rows", summary.get("scored_rows"))),
        ("候選股數", _format_cell("candidate_rows", summary.get("candidate_rows"))),
        ("通過風控數", _format_cell("risk_pass_rows", summary.get("risk_pass_rows"))),
        ("待進場筆數", _format_cell("pending_orders", summary.get("pending_orders"))),
        ("今日成交筆數", _format_cell("executed_orders", summary.get("executed_orders"))),
        ("跳過進場筆數", _format_cell("skipped_orders", summary.get("skipped_orders"))),
        ("目前持倉數", _format_cell("open_positions", summary.get("open_positions"))),
        ("已平倉數", _format_cell("closed_positions", summary.get("closed_positions"))),
        ("未實現損益", _format_cell("unrealized_pnl", summary.get("unrealized_pnl"))),
        ("已實現損益", _format_cell("realized_pnl", summary.get("realized_pnl"))),
        ("總資產", _format_cell("total_equity", summary.get("total_equity"))),
        ("累計交易成本", _format_cell("total_cost", summary.get("total_cost"))),
        ("扣成本後已實現損益", _format_cell("realized_pnl_after_cost", summary.get("realized_pnl_after_cost"))),
        ("扣成本後總資產", _format_cell("total_equity_after_cost", summary.get("total_equity_after_cost"))),
    ]
    return '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _key_takeaway(daily_summary: dict[str, object], paper_summary: dict[str, object]) -> str:
    summary = daily_summary or {}
    performance = paper_summary or {}
    fallback_value = "是" if not _is_blank(summary.get("fallback_date")) else "否"
    cards = [
        ("執行狀態", _format_cell("status", summary.get("status"))),
        ("原始執行日期", _format_cell("requested_date", summary.get("requested_date") or summary.get("trade_date"))),
        ("實際交易日", _format_cell("trade_date", summary.get("trade_date"))),
        ("是否使用最近有效交易日", fallback_value),
        ("候選股數", _format_cell("candidate_rows", summary.get("candidate_rows"))),
        ("通過風控數", _format_cell("risk_pass_rows", summary.get("risk_pass_rows"))),
        ("待進場筆數", _format_cell("pending_orders", summary.get("pending_orders"))),
        ("今日成交筆數", _format_cell("executed_orders", summary.get("executed_orders"))),
        ("跳過進場筆數", _format_cell("skipped_orders", summary.get("skipped_orders"))),
        ("目前持倉數", _format_cell("open_positions", summary.get("open_positions"))),
        ("未實現損益", _format_cell("unrealized_pnl", performance.get("unrealized_pnl", summary.get("unrealized_pnl")))),
        ("已實現損益", _format_cell("realized_pnl", performance.get("realized_pnl", summary.get("realized_pnl")))),
        ("總資產", _format_cell("total_equity", performance.get("total_equity", summary.get("total_equity")))),
        ("扣成本後總資產", _format_cell("total_equity_after_cost", performance.get("total_equity_after_cost", summary.get("total_equity_after_cost")))),
        ("交易成本總額", _format_cell("total_cost", performance.get("total_cost", summary.get("total_cost")))),
    ]
    return '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _health_checks(
    latest_summary: dict[str, object], candidates: pd.DataFrame, risk_pass: pd.DataFrame, pending_orders: pd.DataFrame, paper_trades: pd.DataFrame
) -> list[tuple[str, str, str]]:
    checks: list[tuple[str, str, str]] = []
    checks.append(("最新有效交易日是否正常", "正常" if not _is_blank(latest_summary.get("trade_date")) else "警告", "交易日資料可用" if not _is_blank(latest_summary.get("trade_date")) else "找不到交易日資料"))
    candidate_count = _to_float(latest_summary.get("candidate_rows")) or float(len(candidates))
    checks.append(("候選股數是否為 0", "注意" if candidate_count == 0 else "正常", f"候選股數：{int(candidate_count)}"))
    risk_pass_count = _to_float(latest_summary.get("risk_pass_rows")) or float(len(risk_pass))
    checks.append(("通過風控數是否為 0", "注意" if risk_pass_count == 0 else "正常", f"通過風控數：{int(risk_pass_count)}"))
    stale_pending = _count_stale_pending(pending_orders)
    checks.append(("pending order 是否超過 3 天仍未成交", "警告" if stale_pending > 0 else "正常", f"超過 3 天筆數：{stale_pending}"))
    checks.append(("paper_trades.csv 是否存在", "正常" if not paper_trades.empty else "警告", "已載入" if not paper_trades.empty else "檔案不存在或無資料"))
    checks.append(("reports/index.html 是否成功產生", "正常", "本次產生成功"))
    return checks


def _count_stale_pending(pending_orders: pd.DataFrame) -> int:
    if pending_orders.empty or "status" not in pending_orders.columns:
        return 0
    pending = pending_orders[pending_orders["status"].fillna("").astype(str).str.upper() == "PENDING"].copy()
    if pending.empty or "signal_date" not in pending.columns:
        return 0
    signal_dates = pd.to_datetime(pending["signal_date"], errors="coerce")
    latest = signal_dates.max()
    if pd.isna(latest):
        return 0
    return int((latest - signal_dates).dt.days.gt(3).sum())


def _warning_banner(checks: list[tuple[str, str, str]]) -> str:
    warnings = [title for title, level, _ in checks if level == "警告"]
    if not warnings:
        return ""
    return f'<section class="alert"><h2>⚠️ 重要警告</h2><div>請優先檢查：{escape("、".join(warnings))}</div></section>'


def _health_check_cards(checks: list[tuple[str, str, str]]) -> str:
    cards = []
    for title, level, detail in checks:
        cards.append(f'<div class="card health {escape(level)}"><span>{escape(title)}</span><strong>{escape(level)}</strong><small>{escape(detail)}</small></div>')
    return '<div class="cards">' + "".join(cards) + "</div>"


def _responsive_table_and_cards(frame: pd.DataFrame, columns: list[str], empty_message: str, max_rows: int) -> str:
    table_html = _table(frame, columns, empty_message, max_rows)
    if frame.empty:
        return table_html
    visible_columns = [column for column in columns if column in frame.columns]
    if not visible_columns:
        return table_html
    rows = frame.head(max_rows).copy()
    cards = []
    for _, row in rows.iterrows():
        items = "".join(
            f'<div><span>{escape(COLUMN_LABELS.get(column, column))}</span><strong>{escape(_format_cell(column, row.get(column)))}</strong></div>'
            for column in visible_columns
        )
        cards.append(f'<article class="mobile-card">{items}</article>')
    return table_html + '<div class="mobile-cards">' + "".join(cards) + "</div>"


def _paper_performance(summary: dict[str, object], closed_trades: pd.DataFrame) -> str:
    blocks: list[str] = []
    if summary:
        cards = [
            ("初始資金", _format_cell("total_capital", summary.get("total_capital"))),
            ("投入金額", _format_cell("invested_value", summary.get("invested_value"))),
            ("目前市值", _format_cell("market_value", summary.get("market_value"))),
            ("現金", _format_cell("cash", summary.get("cash"))),
            ("未實現損益", _format_cell("unrealized_pnl", summary.get("unrealized_pnl"))),
            ("已實現損益", _format_cell("realized_pnl", summary.get("realized_pnl"))),
            ("總資產", _format_cell("total_equity", summary.get("total_equity"))),
            ("累計交易成本", _format_cell("total_cost", summary.get("total_cost"))),
            ("扣成本後已實現損益", _format_cell("realized_pnl_after_cost", summary.get("realized_pnl_after_cost"))),
            ("扣成本後總資產", _format_cell("total_equity_after_cost", summary.get("total_equity_after_cost"))),
        ]
        blocks.append('<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>")
    else:
        blocks.append(_empty("目前尚無紙上交易績效資料"))

    blocks.append("<h3>已平倉交易</h3>")
    blocks.append(
        _table(
            closed_trades,
            [
                "trade_date",
                "stock_id",
                "stock_name",
                "entry_price",
                "exit_date",
                "exit_price",
                "exit_commission",
                "exit_tax",
                "total_cost",
                "realized_pnl",
                "realized_pnl_pct",
                "realized_pnl_after_cost",
                "realized_pnl_pct_after_cost",
                "exit_reason",
                "status",
            ],
            "目前尚無已平倉交易",
            max_rows=50,
        )
    )
    return "".join(blocks)


def _exit_strategy_overview(open_positions: pd.DataFrame, closed_trades: pd.DataFrame) -> str:
    combined = pd.concat([open_positions, closed_trades], ignore_index=True) if not (open_positions.empty and closed_trades.empty) else pd.DataFrame()
    if combined.empty:
        return _empty("目前尚無出場策略資料")
    partial1_count = int(pd.to_numeric(combined.get("partial_exit_1_done", 0), errors="coerce").fillna(0).astype(int).sum()) if "partial_exit_1_done" in combined.columns else 0
    trailing_count = int((combined.get("exit_reason", pd.Series(dtype=object)).fillna("").astype(str) == "TRAILING_STOP").sum()) if "exit_reason" in combined.columns else 0
    cards = [
        ("已觸發第一段停利", str(partial1_count)),
        ("移動停利出場筆數", str(trailing_count)),
    ]
    if "remaining_shares" in combined.columns:
        cards.append(("剩餘股數總計", _format_cell("shares", pd.to_numeric(combined["remaining_shares"], errors="coerce").fillna(0).sum())))
    if "highest_price_since_entry" in combined.columns:
        cards.append(("持有期間最高價（平均）", _format_cell("close", pd.to_numeric(combined["highest_price_since_entry"], errors="coerce").dropna().mean() if combined["highest_price_since_entry"].notna().any() else None)))
    if "trailing_stop_price" in combined.columns:
        cards.append(("移動停利線（平均）", _format_cell("close", pd.to_numeric(combined["trailing_stop_price"], errors="coerce").dropna().mean() if combined["trailing_stop_price"].notna().any() else None)))
    return '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _cost_overview(daily_summary: dict[str, object], paper_summary: dict[str, object]) -> str:
    summary = paper_summary or daily_summary
    if not summary:
        return _empty("目前尚無交易成本資料")
    cards = [
        ("累計交易成本", _format_cell("total_cost", summary.get("total_cost"))),
        ("扣成本後已實現損益", _format_cell("realized_pnl_after_cost", summary.get("realized_pnl_after_cost"))),
        ("扣成本後總資產", _format_cell("total_equity_after_cost", summary.get("total_equity_after_cost"))),
    ]
    return '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _fallback_note(summary: dict[str, object]) -> str:
    if not summary:
        return _empty("目前沒有可判斷 fallback 的每日 summary")

    requested = _format_cell("requested_date", summary.get("requested_date") or summary.get("trade_date"))
    actual = _format_cell("trade_date", summary.get("trade_date"))
    fallback = _format_cell("fallback_date", summary.get("fallback_date"))
    reason = _format_cell("fallback_reason", summary.get("fallback_reason"))
    status = _format_cell("status", summary.get("status"))

    if fallback != "-":
        return (
            '<div class="note">'
            f"今日無交易資料，已使用最近有效交易日。原始執行日期：{escape(requested)}；"
            f"實際交易日：{escape(actual)}；使用替代交易日：{escape(fallback)}；"
            f"替代原因：{escape(reason)}；狀態：{escape(status)}。"
            "</div>"
        )
    return '<div class="note">本次未使用替代交易日；若遇非交易日，系統會改用 SQLite 內最近有效交易日。</div>'


def _section(title: str, content: str) -> str:
    return f'<section><h2>{escape(title)}</h2>{content}</section>'


def _card(label: str, value: str) -> str:
    return f'<div class="card"><span>{escape(label)}</span><strong>{escape(value)}</strong></div>'


def _table(frame: pd.DataFrame, columns: list[str], empty_message: str, max_rows: int) -> str:
    if frame.empty:
        return _empty(empty_message)

    visible_columns = [column for column in columns if column in frame.columns]
    if not visible_columns:
        return _empty(empty_message)

    rows = frame.head(max_rows).copy()
    header = "".join(f"<th>{escape(COLUMN_LABELS[column])}</th>" for column in visible_columns)
    body_rows = []
    for _, row in rows.iterrows():
        cells = "".join(
            f"<td>{escape(_format_cell(column, row.get(column)))}</td>" for column in visible_columns
        )
        body_rows.append(f"<tr>{cells}</tr>")
    return '<div class="table-wrap"><table><thead><tr>' + header + "</tr></thead><tbody>" + "".join(body_rows) + "</tbody></table></div>"


def _format_cell(column: str, value: object) -> str:
    if _is_blank(value):
        return "-"
    if str(value).strip() == "NEXT_AVAILABLE_TRADING_DAY":
        return "下一個有效交易日"

    if column == "entry_price_source":
        text = str(value).strip()
        return ENTRY_PRICE_SOURCE_LABELS.get(text, text)
    if column in STATUS_COLUMNS:
        text = str(value).strip()
        return STATUS_LABELS.get(text, text)
    if column in DATE_COLUMNS:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return "-"
        return parsed.strftime("%Y-%m-%d")
    if column in PERCENT_COLUMNS:
        number = _to_float(value)
        if number is None:
            return str(value)
        return f"{number * 100:.2f}%"
    if column in PNL_COLUMNS:
        number = _to_float(value)
        if number is None:
            return str(value)
        return _signed_number(number)
    if column in AMOUNT_COLUMNS:
        number = _to_float(value)
        if number is None:
            return str(value)
        return f"{number:,.0f}"
    if column in PRICE_COLUMNS or column in SCORE_COLUMNS:
        number = _to_float(value)
        if number is None:
            return str(value)
        return f"{number:,.2f}"
    if column in INTEGER_COLUMNS:
        number = _to_float(value)
        if number is None:
            return str(value)
        return f"{number:,.0f}"
    return str(value)


def _signed_number(value: float) -> str:
    if value > 0:
        return f"+{value:,.0f}"
    if value < 0:
        return f"{value:,.0f}"
    return "0"


def _read_latest_csv(report_dir: Path, pattern: str) -> pd.DataFrame:
    latest = _latest_file(report_dir, pattern)
    if latest is None:
        return pd.DataFrame()
    return _read_csv(latest)


def _read_all_csv(report_dir: Path, pattern: str) -> pd.DataFrame:
    frames = [_read_csv(path) for path in _sorted_report_files(report_dir, pattern)]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _read_recent_summaries(report_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in _sorted_report_files(report_dir, "daily_summary_*.csv"):
        frame = _read_csv(path)
        if frame.empty:
            continue
        frame = frame.copy()
        report_date = _date_from_filename(path)
        frame["_report_date"] = report_date or pd.Timestamp.min
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("_report_date", ascending=False)
    return combined.drop(columns=["_report_date"], errors="ignore").reset_index(drop=True)


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _latest_file(report_dir: Path, pattern: str) -> Path | None:
    files = _sorted_report_files(report_dir, pattern)
    return files[0] if files else None


def _sorted_report_files(report_dir: Path, pattern: str) -> list[Path]:
    files = list(report_dir.glob(pattern))
    return sorted(files, key=lambda path: (_date_from_filename(path) or pd.Timestamp.min, path.stat().st_mtime), reverse=True)


def _date_from_filename(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed


def _first_row(frame: pd.DataFrame) -> dict[str, object]:
    if frame.empty:
        return {}
    return frame.iloc[0].to_dict()


def _filter_status(frame: pd.DataFrame, status: str) -> pd.DataFrame:
    if frame.empty or "status" not in frame.columns:
        return pd.DataFrame()
    return frame[frame["status"].fillna("").astype(str).str.upper() == status].copy()


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


def _empty(message: str) -> str:
    return f'<div class="empty">{escape(message)}</div>'


def _css() -> str:
    return """
*{box-sizing:border-box}
body{margin:0;background:#0b1020;color:#e5e7eb;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans TC",sans-serif;line-height:1.6}
.page{width:min(1120px,100%);margin:0 auto;padding:20px}
header{padding:24px 0 18px}
header p{margin:0 0 6px;color:#38bdf8;font-size:14px;font-weight:700}
header h1{margin:0 0 8px;font-size:28px;letter-spacing:0}
header div{color:#94a3b8;font-size:14px}
section{margin:16px 0;padding:18px;background:#111827;border:1px solid #1f2937;border-radius:8px}
h2{margin:0 0 14px;font-size:19px}
h3{margin:18px 0 10px;font-size:16px;color:#cbd5e1}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.card{padding:12px;background:#0f172a;border:1px solid #243244;border-radius:8px}
.card span{display:block;color:#94a3b8;font-size:13px}
.card strong{display:block;margin-top:4px;font-size:18px;color:#f8fafc;word-break:break-word}
.table-wrap{width:100%;overflow-x:auto;border:1px solid #243244;border-radius:8px}
table{width:100%;border-collapse:collapse;min-width:760px;background:#0f172a}
th,td{padding:10px 12px;border-bottom:1px solid #243244;text-align:left;vertical-align:top}
th{color:#bae6fd;background:#172033;font-size:13px;white-space:nowrap}
td{font-size:13px;color:#e5e7eb}
tr:last-child td{border-bottom:0}
.empty,.note{padding:13px;background:#0f172a;border:1px solid #243244;border-radius:8px;color:#cbd5e1}
.note{border-color:#164e63;background:#082f49}
.alert{border-color:#7f1d1d;background:#450a0a}
.mobile-cards{display:none;gap:10px;margin-top:10px}
.mobile-card{padding:12px;background:#0f172a;border:1px solid #243244;border-radius:8px}
.mobile-card div{display:flex;justify-content:space-between;gap:12px;padding:4px 0;border-bottom:1px dashed #243244}
.mobile-card div:last-child{border-bottom:0}
.mobile-card span{font-size:12px;color:#94a3b8}
.mobile-card strong{font-size:13px;color:#f8fafc;text-align:right}
.card.health small{display:block;margin-top:6px;color:#94a3b8}
.card.health.警告 strong{color:#fca5a5}
.card.health.注意 strong{color:#fcd34d}
.card.health.正常 strong{color:#86efac}
@media(max-width:640px){.page{padding:14px}header h1{font-size:24px}section{padding:14px}.card strong{font-size:16px}table{min-width:680px}.table-wrap{display:none}.mobile-cards{display:grid}}
"""


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="產生繁體中文靜態 HTML 報表。")
    parser.add_argument("--reports-dir", default=str(ROOT / "reports"))
    parser.add_argument("--docs-dir", default=str(ROOT / "docs"))
    args = parser.parse_args(list(argv) if argv is not None else None)

    output_path = generate_html_report(args.reports_dir, docs_dir=args.docs_dir)
    print(f"html_report={output_path}")
    print(f"pages_report={Path(args.docs_dir) / 'index.html'}")


if __name__ == "__main__":
    main()
