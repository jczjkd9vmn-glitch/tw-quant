from __future__ import annotations

import argparse
from html import escape
from pathlib import Path
import re
import sys
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tw_quant.config import load_config


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
    "take_profit_exits": "今日停利筆數",
    "stop_loss_exits": "今日停損筆數",
    "trailing_stop_exits": "今日移動停利筆數",
    "trend_exit_exits": "今日趨勢出場筆數",
    "time_exit_exits": "今日時間出場筆數",
    "realized_pnl_after_cost_today": "今日扣成本後已實現損益",
    "fundamental_positive_candidates": "基本面加分候選股數",
    "fundamental_warning_candidates": "基本面警告候選股數",
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
    "revenue_yoy": "月營收 YoY",
    "revenue_mom": "月營收 MoM",
    "accumulated_revenue_yoy": "累計營收 YoY",
    "fundamental_reason": "基本面評分理由",
    "is_candidate": "是否候選",
    "risk_pass": "是否通過風控",
    "risk_reason": "風控原因",
    "reason": "買進理由",
    "stop_loss_price": "停損價",
    "suggested_position_pct": "建議部位",
    "entry_price": "進場價",
    "entry_price_raw": "原始進場價",
    "slippage_rate": "滑價假設",
    "entry_slippage": "進場滑價",
    "buy_slippage_cost": "買進滑價成本",
    "entry_commission": "買進手續費",
    "buy_commission": "買進手續費",
    "shares": "股數",
    "original_shares": "原始股數",
    "remaining_shares": "剩餘股數",
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
    "exit_price_raw": "原始出場價",
    "exit_slippage": "出場滑價",
    "sell_slippage_cost": "賣出滑價成本",
    "exit_commission": "賣出手續費",
    "sell_commission": "賣出手續費",
    "exit_tax": "交易稅",
    "sell_tax": "交易稅",
    "realized_pnl_pct": "已實現損益率",
    "realized_pnl_pct_after_cost": "扣成本後已實現損益率",
    "partial_exit_1_done": "已觸發第一段停利",
    "partial_exit_2_done": "已觸發第二段停利",
    "highest_price_since_entry": "持有期間最高價",
    "highest_pnl_pct_since_entry": "持有期間最高損益率",
    "trailing_stop_price": "移動停利線",
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
    "slippage_rate",
    "unrealized_pnl_pct",
    "realized_pnl_pct",
    "realized_pnl_pct_after_cost",
    "highest_pnl_pct_since_entry",
}
PRICE_COLUMNS = {
    "close",
    "stop_loss_price",
    "entry_price",
    "entry_price_raw",
    "entry_slippage",
    "current_price",
    "exit_price",
    "exit_price_raw",
    "exit_slippage",
    "highest_price_since_entry",
    "trailing_stop_price",
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
    "buy_commission",
    "buy_slippage_cost",
    "exit_commission",
    "sell_commission",
    "sell_slippage_cost",
    "exit_tax",
    "sell_tax",
    "total_cost",
}
PNL_COLUMNS = {
    "unrealized_pnl",
    "realized_pnl",
    "realized_pnl_after_cost",
    "realized_pnl_after_cost_today",
}
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
    "take_profit_exits",
    "stop_loss_exits",
    "trailing_stop_exits",
    "trend_exit_exits",
    "time_exit_exits",
    "fundamental_positive_candidates",
    "fundamental_warning_candidates",
    "shares",
    "original_shares",
    "remaining_shares",
    "holding_days",
}
STATUS_COLUMNS = {
    "status",
    "exit_reason",
    "fallback_reason",
    "is_candidate",
    "risk_pass",
    "stop_loss_hit",
    "partial_exit_1_done",
    "partial_exit_2_done",
}
DATE_COLUMNS = {
    "trade_date",
    "requested_date",
    "fallback_date",
    "exit_date",
    "signal_date",
    "planned_entry_date",
    "actual_entry_date",
}


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
    trading_cost = load_config(ROOT / "config.yaml").get("trading_cost", {})

    html = _render_page(
        report_dir=report_dir,
        daily_summary=daily_summary,
        recent_summaries=recent_summaries,
        candidates=candidates,
        risk_pass=risk_pass,
        paper_trades=paper_trades,
        paper_summary=paper_summary,
        pending_orders=pending_orders,
        trading_cost=trading_cost,
    )

    output_path = report_dir / "index.html"
    output_path.write_text(html, encoding="utf-8")
    if docs_dir is not None:
        docs_path = Path(docs_dir)
        docs_path.mkdir(parents=True, exist_ok=True)
        (docs_path / "index.html").write_text(html, encoding="utf-8")
    return output_path


def _render_page(
    report_dir: Path,
    daily_summary: pd.DataFrame,
    recent_summaries: pd.DataFrame,
    candidates: pd.DataFrame,
    risk_pass: pd.DataFrame,
    paper_trades: pd.DataFrame,
    paper_summary: pd.DataFrame,
    pending_orders: pd.DataFrame,
    trading_cost: dict[str, object],
) -> str:
    latest_summary = _first_row(daily_summary)
    open_positions = _filter_status(paper_trades, "OPEN")
    closed_trades = _filter_status(paper_trades, "CLOSED")
    latest_paper_summary = _first_row(paper_summary)
    open_positions = _enrich_with_fundamentals(open_positions, candidates)
    pending_orders = _enrich_with_fundamentals(pending_orders, candidates)
    closed_trades = _enrich_with_fundamentals(closed_trades, candidates)
    health_items = _health_checks(report_dir, latest_summary, candidates, risk_pass, pending_orders, paper_trades)
    alert = _warning_banner(health_items)
    updated_at = _report_updated_at(report_dir)

    candidate_detail = _table(
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
            "revenue_yoy",
            "revenue_mom",
            "accumulated_revenue_yoy",
            "fundamental_reason",
            "reason",
        ],
        "目前尚無候選股資料",
        max_rows=20,
    )
    risk_pass_detail = _table(
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
    )
    recent_summary_detail = _table(
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
            "take_profit_exits",
            "stop_loss_exits",
            "trailing_stop_exits",
            "trend_exit_exits",
            "realized_pnl_after_cost_today",
            "fundamental_positive_candidates",
            "fundamental_warning_candidates",
        ],
        "目前尚無每日 summary",
        max_rows=10,
    )

    overview_content = "".join(
        [
            _pnl_overview(latest_summary, latest_paper_summary, open_positions),
            _details_block("交易成本摘要", _cost_overview(latest_summary, latest_paper_summary, trading_cost)),
            _details_block("紙上交易績效", _paper_performance(latest_paper_summary, closed_trades)),
            _details_block("出場策略摘要", _exit_strategy_summary(latest_summary, open_positions, closed_trades)),
            _details_block("非交易日替代交易日說明", _fallback_note(latest_summary)),
        ]
    )
    fundamental_content = "".join(
        [
            _fundamental_summary(candidates),
            _details_block("今日候選股詳細表", candidate_detail),
            _details_block("通過風控股票詳細表", risk_pass_detail),
        ]
    )
    health_content = "".join(
        [
            _health_summary_cards(health_items),
            _details_block("系統健康檢查詳細項目", _health_section(health_items)),
            _details_block("最近每日 summary", recent_summary_detail),
        ]
    )

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="zh-Hant">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            "<title>台股紙上交易帳務</title>",
            f"<style>{_css()}</style>",
            "</head>",
            "<body>",
            '<main class="page">',
            _account_header(latest_summary, updated_at),
            alert,
            _nav_tabs(),
            _tab_panel("overview", "總覽", overview_content, active=True),
            _tab_panel("positions", "目前持倉", _position_cards(open_positions)),
            _tab_panel("pending", "待進場", _pending_cards(pending_orders)),
            _tab_panel("closed", "今日 / 最近已出場", _closed_cards(closed_trades)),
            _tab_panel("fundamental", "基本面摘要", fundamental_content),
            _tab_panel("health", "健康檢查", health_content),
            "</main>",
            f"<script>{_javascript()}</script>",
            "</body>",
            "</html>",
        ]
    )


def _account_header(summary: dict[str, object], updated_at: str) -> str:
    requested = _format_cell("requested_date", summary.get("requested_date") or summary.get("trade_date"))
    trade_date = _format_cell("trade_date", summary.get("trade_date"))
    use_recent = "是" if _uses_recent_data(summary) else "否"
    meta = [
        ("原始執行日期", requested),
        ("實際交易日", trade_date),
        ("是否使用最近有效資料", use_recent),
        ("報表更新時間", updated_at or "-"),
    ]
    chips = "".join(f"<span>{escape(label)}：{escape(value)}</span>" for label, value in meta)
    return (
        "<header class=\"account-header\">"
        "<p>台股量化系統</p>"
        "<h1>台股紙上交易帳務</h1>"
        f"<div class=\"header-meta\">{chips}</div>"
        "<small>所有內容僅供紙上模擬交易與策略檢查使用，不代表投資建議，也不保證獲利。</small>"
        "</header>"
    )


def _nav_tabs() -> str:
    tabs = [
        ("overview", "總覽"),
        ("positions", "持倉"),
        ("pending", "待進場"),
        ("closed", "已出場"),
        ("fundamental", "基本面"),
        ("health", "健康檢查"),
    ]
    buttons = []
    for index, (anchor, label) in enumerate(tabs):
        active = " active" if index == 0 else ""
        selected = "true" if index == 0 else "false"
        buttons.append(
            f'<button type="button" class="tab-button{active}" data-tab-target="{anchor}" '
            f'aria-controls="tab-{anchor}" aria-selected="{selected}">{label}</button>'
        )
    return f'<nav class="section-tabs tab-nav" aria-label="報表區塊導覽">{"".join(buttons)}</nav>'


def _tab_panel(panel_id: str, title: str, content: str, active: bool = False) -> str:
    classes = "tab-panel active" if active else "tab-panel"
    return (
        f'<section id="tab-{escape(panel_id)}" class="{classes}" data-tab-panel="{escape(panel_id)}" '
        f'role="tabpanel"><h2>{escape(title)}</h2>{content}</section>'
    )


def _pnl_overview(
    daily_summary: dict[str, object],
    paper_summary: dict[str, object],
    open_positions: pd.DataFrame,
) -> str:
    summary = paper_summary or daily_summary
    if not summary:
        return _empty("目前尚無損益總覽資料")

    market_value = _first_number(summary, "market_value")
    if market_value is None:
        market_value = _sum_column(open_positions, "market_value")
    invested_value = _first_number(summary, "invested_value")
    if invested_value is None:
        invested_value = _sum_column(open_positions, "position_value")
    total_equity_after_cost = _first_number(summary, "total_equity_after_cost") or _first_number(summary, "total_equity")
    total_capital = _first_number(summary, "total_capital")
    unrealized = _first_number(summary, "unrealized_pnl")
    realized = _first_number(summary, "realized_pnl")
    if realized is None:
        realized = _first_number(summary, "realized_pnl_after_cost")
    total_cost = _first_number(summary, "total_cost")
    total_pnl = None
    if total_equity_after_cost is not None and total_capital is not None:
        total_pnl = round(total_equity_after_cost - total_capital, 2)
    elif unrealized is not None or realized is not None:
        total_pnl = round((unrealized or 0.0) + (realized or 0.0), 2)
    return_pct = round(total_pnl / total_capital, 6) if total_pnl is not None and total_capital else None

    primary = [
        ("總現值", _format_number_or_dash(total_equity_after_cost), None, "total-value"),
        ("總成本", _format_number_or_dash(invested_value), None, ""),
        ("總損益", _signed_or_dash(total_pnl), total_pnl, "pnl-main"),
        ("報酬率", _percent_or_dash(return_pct), total_pnl, "pnl-main"),
    ]
    secondary = [
        ("未實現損益", _signed_or_dash(unrealized), unrealized),
        ("累計已實現損益", _signed_or_dash(realized), realized),
        ("累計交易成本", _format_number_or_dash(total_cost), None),
        ("扣成本後總資產", _format_number_or_dash(total_equity_after_cost), None),
    ]
    primary_cards = "".join(_overview_metric(label, value, raw, class_name) for label, value, raw, class_name in primary)
    secondary_cards = "".join(_overview_metric(label, value, raw, "") for label, value, raw in secondary)
    return f'<div class="pnl-card"><h3>損益總覽</h3><div class="pnl-primary">{primary_cards}</div><div class="pnl-secondary">{secondary_cards}</div></div>'


def _overview_metric(label: str, value: str, raw_value: float | None, extra_class: str = "") -> str:
    classes = "overview-metric"
    if extra_class:
        classes += f" {extra_class}"
    value_class = _profit_class(raw_value) if raw_value is not None else "profit-flat"
    return (
        f'<div class="{classes}"><span>{escape(label)}</span>'
        f'<strong class="{value_class}">{escape(value)}</strong></div>'
    )


def _position_cards(frame: pd.DataFrame) -> str:
    if frame.empty:
        return _empty("目前尚無持倉")
    cards = []
    for _, row in frame.iterrows():
        stock_id = _format_cell("stock_id", row.get("stock_id"))
        stock_name = _format_cell("stock_name", row.get("stock_name"))
        pnl = _to_float(row.get("unrealized_pnl"))
        details = _detail_grid(
            row,
            [
                "actual_entry_date",
                "original_shares",
                "remaining_shares",
                "stop_loss_price",
                "partial_exit_1_done",
                "partial_exit_2_done",
                "highest_price_since_entry",
                "trailing_stop_price",
                "entry_price_source",
                "buy_commission",
                "total_cost",
                "fundamental_score",
                "fundamental_reason",
            ],
        )
        metrics = [
            ("剩餘股數", _format_cell("remaining_shares", row.get("remaining_shares") if not _is_blank(row.get("remaining_shares")) else row.get("shares"))),
            ("成交均價", _format_cell("entry_price", row.get("entry_price"))),
            ("最新價格", _format_cell("current_price", row.get("current_price"))),
            ("目前市值", _format_cell("market_value", row.get("market_value"))),
        ]
        metric_html = "".join(f"<div><span>{label}</span><strong>{value}</strong></div>" for label, value in metrics)
        pnl_html = (
            f'<div class="position-pnl pnl-highlight {_profit_class(pnl)}">'
            f'<span>未實現損益</span><strong>{escape(_format_cell("unrealized_pnl", row.get("unrealized_pnl")))}</strong>'
            f'<em>{escape(_format_cell("unrealized_pnl_pct", row.get("unrealized_pnl_pct")))}</em></div>'
        )
        cards.append(
            '<article class="mobile-card position-card">'
            '<div class="holding-head">'
            f'<div><h3>{escape(stock_id)} {escape(stock_name)}</h3><span>{escape(_format_cell("status", row.get("status")))}</span></div>'
            '<b>現股</b>'
            '</div>'
            f'<div class="holding-main">{pnl_html}<div class="holding-metrics">{metric_html}</div></div>'
            f'<details class="card-details"><summary>更多持倉資訊</summary>{details}</details>'
            '</article>'
        )
    table = _table(
        frame,
        [
            "stock_id",
            "stock_name",
            "status",
            "remaining_shares",
            "entry_price",
            "current_price",
            "market_value",
            "unrealized_pnl",
            "unrealized_pnl_pct",
            "stop_loss_price",
        ],
        "目前尚無持倉",
        max_rows=50,
    )
    return (
        '<div class="broker-cards">' + "".join(cards) + "</div>"
        + _details_block("原始持倉資料表格", table, class_name="raw-table-details")
    )


def _pending_cards(frame: pd.DataFrame) -> str:
    if frame.empty:
        return _empty("目前尚無待進場資料")
    cards = []
    for _, row in frame.iterrows():
        stock_id = _format_cell("stock_id", row.get("stock_id"))
        stock_name = _format_cell("stock_name", row.get("stock_name"))
        fields = _detail_grid(
            row,
            [
                "signal_date",
                "planned_entry_date",
                "actual_entry_date",
                "status",
                "signal_close",
                "entry_price",
                "fundamental_score",
                "fundamental_reason",
            ],
        )
        cards.append(
            '<article class="mobile-card pending-card">'
            f'<div class="card-title-row"><h3>{escape(stock_id)} {escape(stock_name)}</h3>'
            f'<span>{escape(_format_cell("status", row.get("status")))}</span></div>'
            f"{fields}</article>"
        )
    table = _table(
        frame,
        ["signal_date", "planned_entry_date", "actual_entry_date", "stock_id", "stock_name", "signal_close", "entry_price", "status", "fundamental_score", "fundamental_reason"],
        "目前尚無待進場資料",
        max_rows=50,
    )
    return (
        '<div class="broker-cards">' + "".join(cards) + "</div>"
        + _details_block("原始待進場資料表格", table, class_name="raw-table-details")
    )


def _closed_cards(frame: pd.DataFrame) -> str:
    if frame.empty:
        return _empty("目前尚無已出場交易")
    cards = []
    for _, row in frame.iterrows():
        stock_id = _format_cell("stock_id", row.get("stock_id"))
        stock_name = _format_cell("stock_name", row.get("stock_name"))
        after_cost = _to_float(row.get("realized_pnl_after_cost"))
        metrics = [
            ("出場日期", _format_cell("exit_date", row.get("exit_date"))),
            ("已實現損益", _format_cell("realized_pnl", row.get("realized_pnl"))),
            ("扣成本後損益", _format_cell("realized_pnl_after_cost", row.get("realized_pnl_after_cost"))),
            ("扣成本後報酬率", _format_cell("realized_pnl_pct_after_cost", row.get("realized_pnl_pct_after_cost"))),
        ]
        metric_html = "".join(f"<div><span>{label}</span><strong>{value}</strong></div>" for label, value in metrics)
        fields = _detail_grid(
            row,
            [
                "exit_reason",
                "exit_price",
                "total_cost",
                "status",
            ],
        )
        cards.append(
            '<article class="mobile-card closed-card">'
            f'<div class="card-title-row"><h3>{escape(stock_id)} {escape(stock_name)}</h3>'
            f'<span>{escape(_format_cell("exit_reason", row.get("exit_reason")))}</span></div>'
            f'<div class="closed-pnl pnl-highlight {_profit_class(after_cost)}"><span>扣成本後已實現損益</span>'
            f'<strong>{escape(_format_cell("realized_pnl_after_cost", row.get("realized_pnl_after_cost")))}</strong></div>'
            f'<div class="holding-metrics closed-metrics">{metric_html}</div>'
            f'<details class="card-details"><summary>更多出場資訊</summary>{fields}</details>'
            "</article>"
        )
    table = _table(
        frame,
        ["stock_id", "stock_name", "exit_date", "exit_reason", "exit_price", "realized_pnl", "realized_pnl_after_cost", "realized_pnl_pct_after_cost", "total_cost", "status"],
        "目前尚無已出場交易",
        max_rows=50,
    )
    return (
        '<div class="broker-cards">' + "".join(cards) + "</div>"
        + _details_block("原始已出場資料表格", table, class_name="raw-table-details")
    )


def _detail_grid(row: pd.Series, columns: list[str]) -> str:
    fields = []
    for column in columns:
        if column not in row.index:
            continue
        value = _format_cell(column, row.get(column))
        if value == "-" and column == "fundamental_reason":
            value = "基本面資料不足，採中性分數"
        fields.append(f"<dt>{escape(COLUMN_LABELS.get(column, column))}</dt><dd>{escape(value)}</dd>")
    return f'<dl class="detail-grid">{"".join(fields)}</dl>'


def _enrich_with_fundamentals(frame: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    columns = ["fundamental_score", "fundamental_reason", "revenue_yoy", "revenue_mom", "accumulated_revenue_yoy"]
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    if candidates.empty or "stock_id" not in candidates.columns:
        result["fundamental_reason"] = result["fundamental_reason"].replace("", "基本面資料不足，採中性分數")
        return result

    candidate_data = candidates.copy()
    candidate_data["stock_id"] = candidate_data["stock_id"].astype(str).str.strip()
    lookup = candidate_data.drop_duplicates("stock_id").set_index("stock_id")
    result["stock_id"] = result["stock_id"].astype(str).str.strip()
    for column in columns:
        if column not in lookup.columns:
            continue
        mapped = result["stock_id"].map(lookup[column])
        current = result[column]
        result[column] = current.where(~current.apply(_is_blank), mapped)
    result["fundamental_reason"] = result["fundamental_reason"].fillna("").replace("", "基本面資料不足，採中性分數")
    return result


def _uses_recent_data(summary: dict[str, object]) -> bool:
    requested = _format_cell("requested_date", summary.get("requested_date") or summary.get("trade_date"))
    actual = _format_cell("trade_date", summary.get("trade_date"))
    if requested != "-" and actual != "-":
        return requested != actual
    return not _is_blank(summary.get("fallback_date"))


def _first_number(summary: dict[str, object], column: str) -> float | None:
    if not summary:
        return None
    return _to_float(summary.get(column))


def _sum_column(frame: pd.DataFrame, column: str) -> float | None:
    if frame.empty or column not in frame.columns:
        return None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None
    return round(float(values.sum()), 2)


def _format_number_or_dash(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number:,.0f}"


def _signed_or_dash(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return _signed_number(number)


def _percent_or_dash(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number * 100:.2f}%"


def _profit_class(value: object) -> str:
    number = _to_float(value)
    if number is None or abs(number) < 0.000001:
        return "profit-flat"
    return "profit-positive" if number > 0 else "profit-negative"


def _report_updated_at(report_dir: Path) -> str:
    files = list(report_dir.glob("*.csv"))
    if not files:
        return ""
    latest_mtime = max(path.stat().st_mtime for path in files)
    return pd.Timestamp(latest_mtime, unit="s", tz="UTC").tz_convert("Asia/Taipei").strftime("%Y-%m-%d %H:%M")


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


def _key_conclusions(summary: dict[str, object]) -> str:
    if not summary:
        return _empty("目前尚無今日重點結論")
    fallback = "是" if not _is_blank(summary.get("fallback_date")) else "否"
    cards = [
        ("執行狀態", _format_cell("status", summary.get("status"))),
        ("原始執行日期", _format_cell("requested_date", summary.get("requested_date") or summary.get("trade_date"))),
        ("實際交易日", _format_cell("trade_date", summary.get("trade_date"))),
        ("是否使用最近有效交易日", fallback),
        ("候選股數", _format_cell("candidate_rows", summary.get("candidate_rows"))),
        ("通過風控數", _format_cell("risk_pass_rows", summary.get("risk_pass_rows"))),
        ("待進場筆數", _format_cell("pending_orders", summary.get("pending_orders"))),
        ("今日成交筆數", _format_cell("executed_orders", summary.get("executed_orders"))),
        ("跳過進場筆數", _format_cell("skipped_orders", summary.get("skipped_orders"))),
        ("目前持倉數", _format_cell("open_positions", summary.get("open_positions"))),
        ("未實現損益", _format_cell("unrealized_pnl", summary.get("unrealized_pnl"))),
        ("已實現損益", _format_cell("realized_pnl", summary.get("realized_pnl"))),
        ("總資產", _format_cell("total_equity", summary.get("total_equity"))),
        ("扣成本後總資產", _format_cell("total_equity_after_cost", summary.get("total_equity_after_cost"))),
        ("交易成本總額", _format_cell("total_cost", summary.get("total_cost"))),
    ]
    return '<div class="cards key-cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _health_checks(
    report_dir: Path,
    summary: dict[str, object],
    candidates: pd.DataFrame,
    risk_pass: pd.DataFrame,
    pending_orders: pd.DataFrame,
    paper_trades: pd.DataFrame,
) -> list[tuple[str, str, str]]:
    trade_date = pd.to_datetime(summary.get("trade_date"), errors="coerce") if summary else pd.NaT
    items = [
        (
            "最新有效交易日",
            "正常" if summary and not pd.isna(trade_date) and summary.get("status") != "FAILED" else "警告",
            _format_cell("trade_date", summary.get("trade_date")) if summary else "找不到每日 summary",
        ),
        (
            "候選股數",
            "警告" if _to_float(summary.get("candidate_rows")) == 0 else "正常",
            f"{_format_cell('candidate_rows', summary.get('candidate_rows'))} 檔",
        ),
        (
            "通過風控數",
            "注意" if _to_float(summary.get("risk_pass_rows")) == 0 else "正常",
            f"{_format_cell('risk_pass_rows', summary.get('risk_pass_rows'))} 檔",
        ),
        (
            "paper_trades.csv",
            "正常" if (report_dir / "paper_trades.csv").exists() or not paper_trades.empty else "注意",
            "已存在" if (report_dir / "paper_trades.csv").exists() or not paper_trades.empty else "尚未建立",
        ),
        (
            "reports/index.html",
            "正常",
            "本次報表已成功產生",
        ),
    ]
    stale = _stale_pending_count(pending_orders, trade_date)
    items.append(
        (
            "pending order 超過 3 天仍未成交",
            "警告" if stale > 0 else "正常",
            f"{stale} 筆",
        )
    )
    return items


def _health_section(items: list[tuple[str, str, str]]) -> str:
    rows = []
    for name, status, detail in items:
        rows.append(
            f'<div class="health {escape(status)}"><strong>{escape(status)}</strong>'
            f"<span>{escape(name)}</span><em>{escape(detail)}</em></div>"
        )
    return '<div class="health-grid">' + "".join(rows) + "</div>"


def _health_summary_cards(items: list[tuple[str, str, str]]) -> str:
    if not items:
        return _empty("目前尚無健康檢查資料")
    warning_count = sum(1 for _, status, _ in items if status == "警告")
    attention_count = sum(1 for _, status, _ in items if status == "注意")
    normal_count = sum(1 for _, status, _ in items if status == "正常")
    status = "警告" if warning_count else "注意" if attention_count else "正常"
    cards = [
        ("整體狀態", status),
        ("正常項目", f"{normal_count:,.0f}"),
        ("注意項目", f"{attention_count:,.0f}"),
        ("警告項目", f"{warning_count:,.0f}"),
    ]
    return '<div class="cards health-summary">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _warning_banner(items: list[tuple[str, str, str]]) -> str:
    warnings = [f"{name}：{detail}" for name, status, detail in items if status == "警告"]
    if not warnings:
        return ""
    return '<div class="top-warning"><strong>警告</strong><span>' + escape("；".join(warnings)) + "</span></div>"


def _stale_pending_count(pending_orders: pd.DataFrame, trade_date: pd.Timestamp) -> int:
    if pending_orders.empty or "status" not in pending_orders.columns or pd.isna(trade_date):
        return 0
    frame = pending_orders[pending_orders["status"].fillna("").astype(str) == "PENDING"].copy()
    if frame.empty or "signal_date" not in frame.columns:
        return 0
    signal_dates = pd.to_datetime(frame["signal_date"], errors="coerce")
    return int(((trade_date - signal_dates).dt.days > 3).fillna(False).sum())


def _fundamental_summary(candidates: pd.DataFrame) -> str:
    if candidates.empty:
        return _empty("基本面資料不足，採中性分數")
    positive = int((pd.to_numeric(candidates.get("fundamental_score"), errors="coerce").fillna(50) > 50).sum())
    warning = int((pd.to_numeric(candidates.get("fundamental_score"), errors="coerce").fillna(50) < 50).sum())
    cards = [
        ("基本面加分候選股數", f"{positive:,.0f}"),
        ("基本面警告候選股數", f"{warning:,.0f}"),
    ]
    table = _table(
        candidates,
        [
            "stock_id",
            "stock_name",
            "fundamental_score",
            "revenue_yoy",
            "revenue_mom",
            "accumulated_revenue_yoy",
            "fundamental_reason",
        ],
        "基本面資料不足，採中性分數",
        max_rows=20,
    )
    return (
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + _details_block("基本面候選股詳細表", table)
    )


def _exit_strategy_summary(
    summary: dict[str, object],
    open_positions: pd.DataFrame,
    closed_trades: pd.DataFrame,
) -> str:
    cards = [
        ("今日停利筆數", _format_cell("take_profit_exits", summary.get("take_profit_exits"))),
        ("今日停損筆數", _format_cell("stop_loss_exits", summary.get("stop_loss_exits"))),
        ("今日移動停利筆數", _format_cell("trailing_stop_exits", summary.get("trailing_stop_exits"))),
        ("今日趨勢出場筆數", _format_cell("trend_exit_exits", summary.get("trend_exit_exits"))),
        ("今日扣成本後已實現損益", _format_cell("realized_pnl_after_cost_today", summary.get("realized_pnl_after_cost_today"))),
    ]
    open_table = _table(
        open_positions,
        [
            "stock_id",
            "stock_name",
            "partial_exit_1_done",
            "remaining_shares",
            "highest_price_since_entry",
            "trailing_stop_price",
            "exit_reason",
        ],
        "目前尚無出場策略持倉資料",
        max_rows=50,
    )
    return (
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + _details_block("出場策略持倉明細", open_table)
    )


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

    blocks.append(
        _details_block(
            "已平倉交易明細",
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
            ),
        )
    )
    return "".join(blocks)


def _cost_overview(
    daily_summary: dict[str, object],
    paper_summary: dict[str, object],
    trading_cost: dict[str, object],
) -> str:
    summary = paper_summary or daily_summary
    if not summary:
        return _empty("目前尚無交易成本資料")
    cards = [
        ("國泰電子下單手續費率", _format_permille(trading_cost.get("commission_rate"))),
        ("最低手續費", f"{_format_amount_plain(trading_cost.get('min_commission'))} 元"),
        ("股票交易稅", _format_rate_percent(trading_cost.get("sell_tax_rate_stock"))),
        ("ETF 交易稅", _format_rate_percent(trading_cost.get("sell_tax_rate_etf"))),
        ("債券 ETF 交易稅", _format_rate_percent(trading_cost.get("sell_tax_rate_bond_etf"))),
        ("滑價假設", _format_rate_percent(trading_cost.get("slippage_rate"))),
        ("累計交易成本", _format_cell("total_cost", summary.get("total_cost"))),
        ("扣成本後已實現損益", _format_cell("realized_pnl_after_cost", summary.get("realized_pnl_after_cost"))),
        ("扣成本後總資產", _format_cell("total_equity_after_cost", summary.get("total_equity_after_cost"))),
    ]
    note = (
        '<div class="note">滑價不是券商費用，而是模擬成交價格偏離理想價格的保守估計。'
        "買進會用較不利的較高成交價，賣出會用較不利的較低成交價。</div>"
    )
    return '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>" + note


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


def _section(title: str, content: str, section_id: str = "", class_name: str = "") -> str:
    id_attr = f' id="{escape(section_id)}"' if section_id else ""
    classes = f' class="{escape(class_name)}"' if class_name else ""
    return f"<section{id_attr}{classes}><h2>{escape(title)}</h2>{content}</section>"


def _details_block(title: str, content: str, open_by_default: bool = False, class_name: str = "") -> str:
    open_attr = " open" if open_by_default else ""
    classes = "collapse-block"
    if class_name:
        classes += f" {class_name}"
    return (
        f'<details class="{escape(classes)}"{open_attr}>'
        f"<summary>{escape(title)}</summary>"
        f'<div class="collapse-content">{content}</div>'
        "</details>"
    )


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


def _responsive_records(frame: pd.DataFrame, columns: list[str], empty_message: str, max_rows: int) -> str:
    table = _table(frame, columns, empty_message, max_rows)
    if frame.empty:
        return table
    visible_columns = [column for column in columns if column in frame.columns]
    cards = []
    for _, row in frame.head(max_rows).iterrows():
        title_parts = [
            _format_cell("stock_id", row.get("stock_id")),
            _format_cell("stock_name", row.get("stock_name")),
        ]
        title = " ".join(part for part in title_parts if part != "-")
        fields = []
        for column in visible_columns:
            if column in {"stock_id", "stock_name"}:
                continue
            fields.append(
                f"<dt>{escape(COLUMN_LABELS[column])}</dt><dd>{escape(_format_cell(column, row.get(column)))}</dd>"
            )
        cards.append(f'<article class="mobile-card"><h3>{escape(title or "持倉")}</h3><dl>{"".join(fields)}</dl></article>')
    return '<div class="mobile-cards">' + "".join(cards) + "</div>" + table


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


def _format_permille(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    text = f"{number * 1000:.3f}".rstrip("0").rstrip(".")
    return f"{text}‰"


def _format_rate_percent(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    text = f"{number * 100:.3f}".rstrip("0").rstrip(".")
    return f"{text}%"


def _format_amount_plain(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "-"
    return f"{number:,.0f}"


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


def _javascript() -> str:
    return """
document.querySelectorAll('[data-tab-target]').forEach(function(button){
  button.addEventListener('click', function(){
    var target = button.getAttribute('data-tab-target');
    document.querySelectorAll('[data-tab-target]').forEach(function(item){
      var active = item === button;
      item.classList.toggle('active', active);
      item.setAttribute('aria-selected', active ? 'true' : 'false');
    });
    document.querySelectorAll('[data-tab-panel]').forEach(function(panel){
      panel.classList.toggle('active', panel.getAttribute('data-tab-panel') === target);
    });
  });
});
"""


def _css() -> str:
    return """
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{margin:0;background:#080d18;color:#e5e7eb;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans TC",sans-serif;line-height:1.6}
.page{width:min(1160px,100%);margin:0 auto;padding:14px}
.account-header{padding:18px 2px 12px}
.account-header p{margin:0 0 4px;color:#38bdf8;font-size:13px;font-weight:700}
.account-header h1{margin:0 0 10px;font-size:26px;letter-spacing:0}
.account-header small{display:block;margin-top:10px;color:#94a3b8;font-size:12px}
.header-meta{display:flex;gap:8px;overflow-x:auto;padding-bottom:2px}
.header-meta span{flex:0 0 auto;padding:5px 9px;border:1px solid #243244;border-radius:999px;background:#0f172a;color:#cbd5e1;font-size:12px}
.section-tabs{position:sticky;top:0;z-index:5;display:flex;gap:8px;overflow-x:auto;margin:10px -14px 12px;padding:9px 14px;background:rgba(8,13,24,.94);backdrop-filter:blur(10px);border-bottom:1px solid #1f2937}
.tab-button{flex:0 0 auto;padding:8px 12px;border:1px solid #243244;border-radius:999px;background:#111827;color:#dbeafe;font:700 14px/1.2 inherit;cursor:pointer}
.tab-button.active{background:#2563eb;border-color:#60a5fa;color:#fff}
.tab-panel{display:none}
.tab-panel.active{display:block}
section{margin:12px 0;padding:14px;background:#101827;border:1px solid #1f2937;border-radius:10px}
h2{margin:0 0 12px;font-size:18px;letter-spacing:0}
h3{margin:0 0 10px;font-size:16px;color:#f8fafc;letter-spacing:0}
.pnl-card{padding:14px;border:1px solid #334155;border-radius:12px;background:linear-gradient(180deg,#172033,#0f172a)}
.pnl-primary{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.pnl-secondary,.cards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:10px}
.overview-metric,.card{padding:12px;background:#0b1220;border:1px solid #243244;border-radius:10px}
.overview-metric span,.card span{display:block;color:#94a3b8;font-size:12px}
.overview-metric strong,.card strong{display:block;margin-top:4px;font-size:17px;color:#f8fafc;word-break:break-word}
.overview-metric.pnl-main strong{font-size:24px}
.overview-metric.total-value strong{font-size:22px}
.profit-positive{color:#f87171!important}
.profit-negative{color:#34d399!important}
.profit-flat{color:#e5e7eb!important}
.broker-cards{display:grid;gap:12px}
.mobile-card{padding:14px;background:#0f172a;border:1px solid #263244;border-radius:12px;box-shadow:0 10px 24px rgba(0,0,0,.16)}
.holding-head,.card-title-row{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.holding-head span,.card-title-row span{display:inline-block;color:#cbd5e1;font-size:13px}
.holding-head b{padding:3px 8px;border-radius:999px;background:#1e3a8a;color:#dbeafe;font-size:12px;white-space:nowrap}
.holding-main{display:grid;gap:12px;margin-top:12px}
.position-pnl,.closed-pnl{padding:14px;border-radius:12px;background:#111827;border:1px solid #243244}
.position-pnl span,.closed-pnl span{display:block;color:#94a3b8;font-size:12px}
.position-pnl strong,.closed-pnl strong{display:block;font-size:30px;font-weight:800;letter-spacing:0}
.position-pnl em{display:block;font-style:normal;font-size:15px;font-weight:700}
.pnl-highlight{min-height:96px;display:flex;flex-direction:column;justify-content:center}
.holding-metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
.holding-metrics div{padding:10px;border-radius:8px;background:#0b1220;border:1px solid #1f2937}
.holding-metrics span{display:block;color:#94a3b8;font-size:12px}
.holding-metrics strong{font-size:15px;color:#f8fafc}
.card-details{margin-top:12px;border-top:1px solid #243244;padding-top:10px}
.collapse-block{margin-top:12px;border:1px solid #243244;border-radius:10px;background:#0b1220}
.collapse-block>summary{padding:12px 13px;color:#bfdbfe;font-size:14px;font-weight:800;cursor:pointer;list-style:none}
.collapse-block>summary::-webkit-details-marker{display:none}
.collapse-block>summary:after{content:"展開";float:right;color:#94a3b8;font-size:12px;font-weight:700}
.collapse-block[open]>summary:after{content:"收合"}
.collapse-content{padding:0 12px 12px}
summary{cursor:pointer;color:#bfdbfe;font-size:14px;font-weight:700}
.detail-grid{display:grid;grid-template-columns:120px 1fr;gap:7px 10px;margin:10px 0 0}
.detail-grid dt{color:#94a3b8;font-size:12px}.detail-grid dd{margin:0;color:#e5e7eb;font-size:13px}
.table-wrap{display:none;width:100%;overflow-x:auto;border:1px solid #243244;border-radius:8px;margin-top:12px}
.collapse-block .table-wrap{display:block}
table{width:100%;border-collapse:collapse;min-width:760px;background:#0f172a}
th,td{padding:10px 12px;border-bottom:1px solid #243244;text-align:left;vertical-align:top}
th{color:#bae6fd;background:#172033;font-size:13px;white-space:nowrap}
td{font-size:13px;color:#e5e7eb}
tr:last-child td{border-bottom:0}
.empty,.note{padding:13px;background:#0f172a;border:1px solid #243244;border-radius:10px;color:#cbd5e1}
.note{border-color:#164e63;background:#082f49;margin-top:10px}
.top-warning{display:flex;gap:10px;align-items:flex-start;margin:10px 0;padding:12px;background:#7f1d1d;border:1px solid #ef4444;border-radius:10px;color:#fee2e2}
.top-warning strong{white-space:nowrap}
.health-grid{display:grid;grid-template-columns:1fr;gap:10px}
.health{padding:12px;background:#0f172a;border:1px solid #243244;border-radius:10px}
.health strong{display:inline-block;margin-right:8px;padding:2px 8px;border-radius:999px;font-size:12px}
.health span,.health em{display:block;margin-top:5px;font-style:normal}
.health.正常 strong{background:#065f46;color:#d1fae5}.health.注意 strong{background:#854d0e;color:#fef3c7}.health.警告 strong{background:#991b1b;color:#fee2e2}
@media(min-width:760px){.page{padding:22px}.account-header h1{font-size:32px}.section-tabs{margin:12px 0 16px;padding:10px 0}.pnl-primary{grid-template-columns:repeat(4,minmax(0,1fr))}.pnl-secondary,.cards{grid-template-columns:repeat(auto-fit,minmax(160px,1fr))}.holding-main{grid-template-columns:220px 1fr}.broker-cards{grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}.health-grid{grid-template-columns:repeat(auto-fit,minmax(220px,1fr))}}
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
