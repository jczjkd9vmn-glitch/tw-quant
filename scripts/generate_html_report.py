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
    "high_risk_event_candidates": "高風險事件警告數",
    "valuation_warning_candidates": "估值警告候選股數",
    "financial_warning_candidates": "財報警告候選股數",
    "institutional_positive_candidates": "籌碼加分候選股數",
    "multi_factor_data_status": "多因子資料更新狀態",
    "total_capital": "初始資金",
    "invested_value": "投入金額",
    "market_value": "目前市值",
    "cash": "現金",
    "stock_id": "股票代號",
    "stock_name": "股票名稱",
    "close": "收盤價",
    "total_score": "總分",
    "original_total_score": "原始總分",
    "multi_factor_score": "多因子分數",
    "multi_factor_reason": "多因子理由",
    "trend_score": "趨勢分數",
    "momentum_score": "動能分數",
    "fundamental_score": "基本面分數",
    "chip_score": "籌碼分數",
    "risk_score": "風險分數",
    "revenue_yoy": "月營收 YoY",
    "revenue_mom": "月營收 MoM",
    "accumulated_revenue_yoy": "累計營收 YoY",
    "revenue_score": "月營收分數",
    "revenue_reason": "月營收理由",
    "fundamental_reason": "基本面評分理由",
    "valuation_score": "估值分數",
    "pe_ratio": "本益比 PE",
    "pb_ratio": "股價淨值比 PB",
    "dividend_yield": "殖利率",
    "valuation_reason": "估值理由",
    "valuation_warning": "估值警告",
    "financial_score": "財報分數",
    "eps": "EPS",
    "roe": "ROE",
    "gross_margin": "毛利率",
    "operating_margin": "營益率",
    "debt_ratio": "負債比",
    "financial_reason": "財報理由",
    "financial_warning": "財報警告",
    "event_score": "重大訊息分數",
    "event_reason": "重大訊息理由",
    "event_risk_level": "事件風險等級",
    "event_blocked": "是否阻擋新進場",
    "institutional_score": "籌碼分數",
    "foreign_net_buy": "外資買賣超",
    "investment_trust_net_buy": "投信買賣超",
    "dealer_net_buy": "自營商買賣超",
    "institutional_reason": "籌碼理由",
    "is_candidate": "是否候選",
    "risk_pass": "通過風控",
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
    "exit_type": "出場類型",
    "recent_partial_exit_reason": "最近部分出場原因",
    "market_intel_status": "市場判斷狀態",
    "market_intel_warning_count": "市場情報資料不足股票數",
    "market_intel_top_score": "市場判斷最高分",
    "market_intel_source": "市場判斷來源",
    "market_intel_warning": "市場判斷警告",
    "market_close": "市場資料收盤價",
    "market_volume": "市場資料成交量",
    "volume_change_ratio": "量能變化",
    "market_pe_ratio": "市場資料 PE",
    "market_pb_ratio": "市場資料 PB",
    "market_dividend_yield": "市場資料殖利率",
    "market_revenue_growth_yoy": "市場資料營收 YoY",
    "market_eps_growth_yoy": "市場資料 EPS YoY",
    "latest_news_titles": "市場判斷來源文字",
    "matched_news_keywords": "新聞命中關鍵字",
    "news_sentiment_score": "新聞情緒分數",
    "market_fundamental_score": "市場基本面分數",
    "market_valuation_score": "市場估值分數",
    "market_momentum_score": "市場動能分數",
    "final_market_score": "市場綜合分數",
    "confidence_score": "信心分數",
    "market_risk_score": "市場風險分數",
    "risk_flags": "主要風險標籤",
    "final_comment": "系統短評",
    "error_step": "失敗步驟",
    "error_message": "錯誤訊息",
}


COLUMN_LABELS.update(
    {
        "market_chip_score": "市場籌碼分數",
        "credit_score": "信用健康分數",
        "event_risk_score": "事件風險健康分數",
        "liquidity_score": "流動性分數",
        "sector_strength_score": "產業相對強弱分數",
        "data_source_warning": "資料來源警告",
        "system_comment": "系統短評",
        "monthly_revenue": "月營收",
        "revenue_3m_trend": "月營收 3 個月趨勢",
        "revenue_12m_high": "月營收 12 個月新高",
        "revenue_warning": "月營收警告",
        "total_institutional_net_buy": "三大法人合計買賣超",
        "foreign_buy_days": "外資連買天數",
        "investment_trust_buy_days": "投信連買天數",
        "institutional_buy_ratio": "法人買超占成交量",
        "institutional_warning": "法人籌碼警告",
        "margin_balance": "融資餘額",
        "margin_change": "融資增減",
        "short_balance": "融券餘額",
        "short_change": "融券增減",
        "securities_lending_sell_volume": "借券賣出量",
        "securities_lending_balance": "借券餘額",
        "margin_usage_warning": "融資使用警告",
        "short_selling_warning": "放空壓力警告",
        "is_attention_stock": "是否注意股",
        "attention_reason": "注意股原因",
        "is_disposition_stock": "是否處置股",
        "disposition_start_date": "處置開始日",
        "disposition_end_date": "處置結束日",
        "disposition_reason": "處置原因",
        "event_keywords": "事件關鍵字",
        "event_warning": "事件警告",
        "industry": "產業",
        "stock_return_5d": "個股 5 日報酬",
        "stock_return_20d": "個股 20 日報酬",
        "market_return_5d": "大盤 5 日報酬",
        "market_return_20d": "大盤 20 日報酬",
        "sector_return_5d": "產業 5 日報酬",
        "sector_return_20d": "產業 20 日報酬",
        "relative_strength_5d": "5 日相對強弱",
        "relative_strength_20d": "20 日相對強弱",
        "sector_strength_rank": "產業強度排名",
        "sector_strength_reason": "產業強弱理由",
        "avg_volume_20d": "20 日均量",
        "avg_turnover_20d": "20 日均成交金額",
        "intraday_trading_ratio": "當日量能倍數",
        "liquidity_warning": "流動性警告",
        "slippage_risk_score": "滑價風險分數",
    }
)

STATUS_LABELS = {
    "OK": "成功",
    "OK_WITH_FALLBACK": "成功，使用最近有效交易日",
    "OK_WITH_WARNING": "成功但有資料警告",
    "CACHE": "使用快取資料",
    "MISSING": "資料缺失",
    "EMPTY": "無資料",
    "DISABLED": "已停用",
    "FAILED": "失敗",
    "OPEN": "持有中",
    "CLOSED": "已出場",
    "STOP_LOSS": "停損",
    "TAKE_PROFIT_1": "第一段停利",
    "TAKE_PROFIT_2": "第二段停利",
    "TRAILING_STOP": "移動停利",
    "MA_EXIT": "跌破 20 日均線",
    "TIME_EXIT": "持有過久出場",
    "stop_loss": "停損",
    "take_profit_1": "第一段停利",
    "take_profit_2": "第二段停利",
    "trailing_stop": "移動停利",
    "ma20_break": "跌破 20 日均線",
    "max_holding_days": "持有過久出場",
    "manual_or_legacy": "手動或舊版出場",
    "error": "錯誤",
    "pending_entry": "等待隔日進場",
    "open": "持有中",
    "closed": "已出場",
    "skipped": "略過",
    "no_signal": "無訊號",
    "PENDING": "等待進場",
    "EXECUTED": "已成交",
    "SKIPPED_EXISTING_POSITION": "已有持倉，略過重複進場",
    "OPEN": "持有中",
    "no trading data": "無交易資料",
    "HIGH": "高",
    "MEDIUM": "中",
    "LOW": "低",
    "NONE": "無",
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
    "original_total_score",
    "multi_factor_score",
    "revenue_score",
    "valuation_score",
    "financial_score",
    "event_score",
    "institutional_score",
    "market_fundamental_score",
    "market_valuation_score",
    "market_momentum_score",
    "final_market_score",
    "confidence_score",
    "market_risk_score",
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
    "market_intel_warning_count",
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
    "event_risk_level",
    "event_blocked",
    "market_intel_status",
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
SCORE_COLUMNS.update(
    {
        "market_chip_score",
        "credit_score",
        "event_risk_score",
        "liquidity_score",
        "sector_strength_score",
        "slippage_risk_score",
    }
)
PERCENT_COLUMNS.update(
    {
        "institutional_buy_ratio",
        "stock_return_5d",
        "stock_return_20d",
        "market_return_5d",
        "market_return_20d",
        "sector_return_5d",
        "sector_return_20d",
        "relative_strength_5d",
        "relative_strength_20d",
    }
)
AMOUNT_COLUMNS.update({"monthly_revenue", "avg_turnover_20d"})
INTEGER_COLUMNS.update(
    {
        "foreign_buy_days",
        "investment_trust_buy_days",
        "margin_balance",
        "margin_change",
        "short_balance",
        "short_change",
        "securities_lending_sell_volume",
        "securities_lending_balance",
        "avg_volume_20d",
    }
)
STATUS_COLUMNS.update({"is_attention_stock", "is_disposition_stock", "revenue_12m_high"})
DATE_COLUMNS.update({"disposition_start_date", "disposition_end_date"})


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
    market_intel = _read_latest_csv(report_dir, "market_intel_*.csv")
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
        market_intel=market_intel,
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
    market_intel: pd.DataFrame,
    trading_cost: dict[str, object],
) -> str:
    latest_summary = _first_row(daily_summary)
    data_fetch_status = _read_latest_csv(report_dir, "data_fetch_status_*.csv")
    candidates = _normalize_attention_disposition_display(candidates)
    risk_pass = _normalize_attention_disposition_display(_enrich_with_fundamentals(risk_pass, candidates))
    market_intel = _normalize_attention_disposition_display(market_intel)
    enrichment_source = _combined_enrichment_sources(candidates, risk_pass, market_intel)
    open_positions = _filter_status(paper_trades, "OPEN")
    closed_trades = _filter_status(paper_trades, "CLOSED")
    latest_paper_summary = _first_row(paper_summary)
    open_positions = _mark_missing_market_context(_enrich_with_fundamentals(open_positions, enrichment_source), enrichment_source)
    pending_orders = _enrich_with_fundamentals(pending_orders, enrichment_source)
    closed_trades = _enrich_with_fundamentals(closed_trades, enrichment_source)
    health_items = _health_checks(
        report_dir,
        latest_summary,
        candidates,
        risk_pass,
        pending_orders,
        paper_trades,
        market_intel,
        data_fetch_status,
    )
    alert = _warning_banner(health_items)
    updated_at = _report_updated_at(report_dir)

    candidate_detail = _responsive_records(
        candidates,
        [
            "rank",
            "trade_date",
            "stock_id",
            "stock_name",
            "close",
            "total_score",
            "original_total_score",
            "multi_factor_score",
            "trend_score",
            "momentum_score",
            "risk_score",
            "revenue_score",
            "revenue_yoy",
            "revenue_mom",
            "accumulated_revenue_yoy",
            "revenue_reason",
            "valuation_score",
            "pe_ratio",
            "pb_ratio",
            "dividend_yield",
            "valuation_warning",
            "financial_score",
            "eps",
            "roe",
            "financial_warning",
            "event_score",
            "event_risk_level",
            "event_blocked",
            "is_attention_stock",
            "attention_reason",
            "is_disposition_stock",
            "disposition_reason",
            "event_reason",
            "institutional_score",
            "institutional_reason",
            "credit_score",
            "event_risk_score",
            "liquidity_score",
            "sector_strength_score",
            "data_source_warning",
            "system_comment",
            "market_fundamental_score",
            "market_valuation_score",
            "market_momentum_score",
            "market_chip_score",
            "news_sentiment_score",
            "final_market_score",
            "confidence_score",
            "risk_flags",
            "final_comment",
            "multi_factor_reason",
            "reason",
        ],
        "目前尚無候選股資料",
        max_rows=20,
    )
    risk_pass_detail = _responsive_records(
        risk_pass,
        [
            "rank",
            "stock_id",
            "stock_name",
            "close",
            "total_score",
            "multi_factor_score",
            "final_market_score",
            "confidence_score",
            "institutional_score",
            "credit_score",
            "event_risk_score",
            "liquidity_score",
            "sector_strength_score",
            "risk_flags",
            "final_comment",
            "is_attention_stock",
            "attention_reason",
            "is_disposition_stock",
            "disposition_reason",
            "event_reason",
            "event_blocked",
            "event_risk_level",
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
            "high_risk_event_candidates",
            "valuation_warning_candidates",
            "financial_warning_candidates",
            "institutional_positive_candidates",
            "multi_factor_data_status",
        ],
        "目前尚無每日 summary",
        max_rows=10,
    )

    overview_content = "".join(
        [
            _section("今日重點結論", _key_conclusions_v2(latest_summary, data_fetch_status), class_name="key-conclusion-section"),
            _pnl_overview(latest_summary, latest_paper_summary, open_positions),
            _details_block("交易成本摘要", _cost_overview(latest_summary, latest_paper_summary, trading_cost)),
            _details_block("紙上交易績效", _paper_performance(latest_paper_summary, closed_trades, open_positions)),
            _details_block("出場策略摘要", _exit_strategy_summary(latest_summary, open_positions, closed_trades)),
            _details_block("非交易日替代交易日說明", _fallback_note(latest_summary)),
        ]
    )
    fundamental_content = "".join(
        [
            _data_confidence_summary(candidates, market_intel, latest_summary, data_fetch_status),
            _market_intel_summary(candidates, market_intel, latest_summary),
            _multi_factor_summary(candidates, latest_summary),
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
            _account_header_v2(latest_summary, updated_at),
            alert,
            _nav_tabs_v2(),
            _tab_panel("overview", "總覽", overview_content, active=True),
            _tab_panel("positions", "目前持倉", _position_cards(open_positions)),
            _tab_panel("pending", "待進場", _pending_cards(pending_orders)),
            _tab_panel("closed", "今日 / 最近已出場", _closed_cards(closed_trades)),
            _tab_panel("fundamental", "市場情報 / 多因子", fundamental_content),
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
        ("fundamental", "市場情報 / 多因子"),
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


def _account_header_v2(summary: dict[str, object], updated_at: str) -> str:
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
        '<header class="account-header">'
        "<p>台股自動化系統</p>"
        "<h1>台股紙上交易帳務</h1>"
        f'<div class="header-meta">{chips}</div>'
        "<small>本頁為紙上交易帳務與風控檢查報表，不代表投資建議，不保證獲利。</small>"
        "</header>"
    )


def _nav_tabs_v2() -> str:
    tabs = [
        ("overview", "總覽"),
        ("positions", "持倉"),
        ("pending", "待進場"),
        ("closed", "已出場"),
        ("fundamental", "市場情報 / 多因子"),
        ("health", "健康檢查"),
    ]
    buttons = []
    for index, (anchor, label) in enumerate(tabs):
        active = " active" if index == 0 else ""
        selected = "true" if index == 0 else "false"
        buttons.append(
            f'<button type="button" class="tab-button{active}" data-tab-target="{anchor}" '
            f'aria-controls="tab-{anchor}" aria-selected="{selected}">{escape(label)}</button>'
        )
    return f'<nav class="section-tabs tab-nav" aria-label="報表區塊導覽">{"".join(buttons)}</nav>'


def _tab_panel(panel_id: str, title: str, content: str, active: bool = False) -> str:
    classes = "tab-panel active" if active else "tab-panel"
    title = {
        "overview": "總覽",
        "positions": "目前持倉",
        "pending": "待進場",
        "closed": "今日 / 最近已出場",
        "fundamental": "市場情報 / 多因子",
        "health": "系統健康檢查",
    }.get(panel_id, title)
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
        ("帳戶總資產", _format_number_or_dash(total_equity_after_cost), None, "total-value"),
        ("目前持倉投入成本", _format_number_or_dash(invested_value), None, ""),
        ("相對初始資金損益", _signed_or_dash(total_pnl), total_pnl, "pnl-main"),
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
        details = _position_detail_grid(row)
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
    waiting = frame[frame["status"].fillna("").astype(str).str.upper() == "PENDING"].copy() if "status" in frame.columns else frame.copy()
    skipped = frame[
        frame["status"].fillna("").astype(str).str.upper().str.contains("SKIPPED|SKIP", regex=True)
    ].copy() if "status" in frame.columns else pd.DataFrame()
    summary = '<div class="cards">' + _card("等待進場", f"{len(waiting):,.0f}") + _card("已略過", f"{len(skipped):,.0f}") + "</div>"
    waiting_cards = _pending_card_list(waiting, "目前尚無等待進場資料")
    skipped_cards = _pending_card_list(skipped, "目前尚無已略過進場資料")
    table = _table(
        frame,
        ["signal_date", "planned_entry_date", "actual_entry_date", "stock_id", "stock_name", "signal_close", "entry_price", "status", "fundamental_score", "fundamental_reason", "skipped_reason"],
        "目前尚無待進場資料",
        max_rows=50,
    )
    return (
        summary
        + "<h3>等待進場</h3>"
        + waiting_cards
        + "<h3>已略過</h3>"
        + skipped_cards
        + _details_block("原始待進場資料表格", table, class_name="raw-table-details")
    )


def _pending_card_list(frame: pd.DataFrame, empty_message: str) -> str:
    if frame.empty:
        return _empty(empty_message)
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
                "institutional_score",
                "credit_score",
                "event_risk_score",
                "liquidity_score",
                "sector_strength_score",
                "final_market_score",
                "confidence_score",
                "risk_flags",
                "final_comment",
            ],
        )
        cards.append(
            '<article class="mobile-card pending-card">'
            f'<div class="card-title-row"><h3>{escape(stock_id)} {escape(stock_name)}</h3>'
            f'<span>{escape(_format_cell("status", row.get("status")))}</span></div>'
            f"{fields}</article>"
        )
    return '<div class="broker-cards">' + "".join(cards) + "</div>"


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


def _position_detail_grid(row: pd.Series) -> str:
    partial_exit = _is_open_partial_exit(row)
    fields: list[tuple[str, str]] = [
        ("實際進場日", _legacy_actual_entry_date(row)),
        ("原始股數", _format_cell("original_shares", row.get("original_shares"))),
        ("剩餘股數", _format_cell("remaining_shares", row.get("remaining_shares"))),
        ("停損價", _format_cell("stop_loss_price", row.get("stop_loss_price"))),
        ("第一段停利是否已觸發", _format_cell("partial_exit_1_done", row.get("partial_exit_1_done"))),
        ("第二段停利是否已觸發", _format_cell("partial_exit_2_done", row.get("partial_exit_2_done"))),
        ("持有期間最高價", _format_cell("highest_price_since_entry", row.get("highest_price_since_entry"))),
        ("移動停利線", _format_cell("trailing_stop_price", row.get("trailing_stop_price"))),
        ("成交價格來源", _legacy_entry_price_source(row)),
        ("買進手續費", _legacy_cost_cell(row, "buy_commission")),
        ("累計成本", _legacy_cost_cell(row, "total_cost")),
    ]
    if partial_exit:
        fields.extend(
            [
                ("最近部分出場原因", _format_cell("exit_reason", row.get("exit_reason"))),
                ("最近部分出場日期", _format_cell("exit_date", row.get("exit_date"))),
            ]
        )
    for column in [
        "fundamental_score",
        "fundamental_reason",
        "multi_factor_score",
        "institutional_score",
        "credit_score",
        "event_risk_score",
        "liquidity_score",
        "sector_strength_score",
        "final_market_score",
        "confidence_score",
        "market_intel_source",
        "market_intel_warning",
        "risk_flags",
        "final_comment",
        "data_source_warning",
        "event_risk_level",
        "event_reason",
        "event_blocked",
    ]:
        if column in row.index:
            fields.append((COLUMN_LABELS.get(column, column), _format_cell(column, row.get(column))))
    body = "".join(f"<dt>{escape(label)}</dt><dd>{escape(value)}</dd>" for label, value in fields)
    return f'<dl class="detail-grid">{body}</dl>'


def _is_open_partial_exit(row: pd.Series) -> bool:
    status = str(row.get("status", "")).strip().upper()
    reason = str(row.get("exit_reason", "")).strip().upper()
    return status == "OPEN" and reason in {"TAKE_PROFIT_1", "TAKE_PROFIT_2"}


def _is_legacy_entry_missing(row: pd.Series) -> bool:
    return _is_blank(row.get("actual_entry_date")) or _is_blank(row.get("entry_price_source"))


def _legacy_actual_entry_date(row: pd.Series) -> str:
    if not _is_blank(row.get("actual_entry_date")):
        return _format_cell("actual_entry_date", row.get("actual_entry_date"))
    fallback = _format_cell("trade_date", row.get("trade_date"))
    return f"{fallback}（舊資料 fallback）" if fallback != "-" else "舊資料未記錄"


def _legacy_entry_price_source(row: pd.Series) -> str:
    if _is_blank(row.get("entry_price_source")):
        return "舊資料未記錄"
    return _format_cell("entry_price_source", row.get("entry_price_source"))


def _legacy_cost_cell(row: pd.Series, column: str) -> str:
    value = row.get(column)
    if _is_legacy_entry_missing(row) and (_is_blank(value) or (_to_float(value) or 0) == 0):
        return "舊資料未記錄"
    return _format_cell(column, value)


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


def _combined_enrichment_sources(*frames: pd.DataFrame) -> pd.DataFrame:
    usable = [frame.copy() for frame in frames if not frame.empty and "stock_id" in frame.columns]
    if not usable:
        return pd.DataFrame()
    combined = pd.concat(usable, ignore_index=True, sort=False)
    combined["stock_id"] = combined["stock_id"].astype(str).str.strip()
    return combined.drop_duplicates("stock_id", keep="first")


def _enrich_with_fundamentals(frame: pd.DataFrame, candidates: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    columns = [
        "fundamental_score",
        "fundamental_reason",
        "revenue_yoy",
        "revenue_mom",
        "accumulated_revenue_yoy",
        "multi_factor_score",
        "multi_factor_reason",
        "event_risk_level",
        "event_reason",
        "event_blocked",
        "market_fundamental_score",
        "market_valuation_score",
        "market_momentum_score",
        "market_chip_score",
        "credit_score",
        "event_risk_score",
        "liquidity_score",
        "sector_strength_score",
        "news_sentiment_score",
        "final_market_score",
        "confidence_score",
        "risk_flags",
        "final_comment",
        "data_source_warning",
        "market_intel_warning",
        "market_intel_source",
    ]
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


def _mark_missing_market_context(frame: pd.DataFrame, enrichment_source: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "stock_id" not in frame.columns:
        return frame
    result = frame.copy()
    known_ids = set()
    if not enrichment_source.empty and "stock_id" in enrichment_source.columns:
        known_ids = set(enrichment_source["stock_id"].astype(str).str.strip())
    for column in ["final_comment", "market_intel_warning", "data_source_warning"]:
        if column not in result.columns:
            result[column] = ""
        result[column] = result[column].astype("object")
    missing_mask = ~result["stock_id"].astype(str).str.strip().isin(known_ids)
    message = "今日未入選候選股，暫無最新多因子資料"
    for column in ["final_comment", "market_intel_warning", "data_source_warning"]:
        result.loc[missing_mask & result[column].apply(_is_blank), column] = message
    return result


def _normalize_attention_disposition_display(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    for column in ["risk_flags", "system_comment", "event_reason"]:
        if column not in result.columns:
            result[column] = ""
        result[column] = result[column].astype("object")
        result[column] = result[column].where(~result[column].apply(_is_blank), "")

    for index, row in result.iterrows():
        attention = _truthy(row.get("is_attention_stock"))
        disposition = _truthy(row.get("is_disposition_stock"))
        if attention:
            reason = _clean_text(row.get("attention_reason")) or "原因未記錄"
            result.at[index, "risk_flags"] = _append_unique_text(row.get("risk_flags"), "注意股")
            result.at[index, "system_comment"] = _append_unique_text(
                row.get("system_comment"),
                "注意股，短線波動風險偏高，預設不阻擋但需人工確認",
            )
            result.at[index, "event_reason"] = f"注意股：{reason}"
        if disposition:
            reason = _clean_text(row.get("disposition_reason")) or "原因未記錄"
            result.at[index, "risk_flags"] = _append_unique_text(result.at[index, "risk_flags"], "處置股")
            result.at[index, "system_comment"] = _append_unique_text(
                result.at[index, "system_comment"],
                "處置股，預設阻擋新增進場",
            )
            result.at[index, "event_reason"] = f"處置股：{reason}"
    return result


def _truthy(value: object) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}


def _clean_text(value: object) -> str:
    if _is_blank(value):
        return ""
    text = str(value).strip()
    return "" if text == "-" else text


def _append_unique_text(base: object, addition: str) -> str:
    text = _clean_text(base)
    if not text:
        return addition
    if addition in text:
        return text
    return f"{text}；{addition}"


def _uses_recent_data(summary: dict[str, object]) -> bool:
    requested = _normalized_date_text(summary.get("requested_date"))
    fallback = _normalized_date_text(summary.get("fallback_date"))
    return bool(requested and fallback and requested != fallback)


def _normalized_date_text(value: object) -> str:
    if _is_blank(value):
        return ""
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        text = str(value).strip()
        return "" if text == "-" else text
    return parsed.strftime("%Y-%m-%d")


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
        return "profit-flat neutral"
    return "profit-positive positive" if number > 0 else "profit-negative negative"


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


def _key_conclusions_v2(summary: dict[str, object], data_fetch_status: pd.DataFrame) -> str:
    if not summary:
        return _empty("今日無重點結論資料")
    fallback_active = _uses_recent_data(summary)
    day_label = "資料交易日" if fallback_active else "今日日期"
    prefix = "最近有效交易日" if fallback_active else "今日"
    cards = [
        (day_label, _format_cell("trade_date", summary.get("trade_date"))),
        (f"{prefix}候選股數量", _format_cell("candidate_rows", summary.get("candidate_rows"))),
        (f"{prefix}通過風控股票數量", _format_cell("risk_pass_rows", summary.get("risk_pass_rows"))),
        (f"{prefix} pending orders 數量", _format_cell("pending_orders", summary.get("pending_orders"))),
        (f"{prefix} open positions 數量", _format_cell("open_positions", summary.get("open_positions"))),
        (f"{prefix} closed trades 數量", _format_cell("closed_positions", summary.get("closed_positions"))),
        (f"{prefix} market intelligence 狀態", _format_cell("market_intel_status", summary.get("market_intel_status"))),
        ("資料品質摘要", _data_quality_summary(summary, data_fetch_status)),
    ]
    return '<div class="cards key-cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _data_quality_summary(summary: dict[str, object], data_fetch_status: pd.DataFrame) -> str:
    if not summary:
        return "缺少每日 summary"
    issues: list[str] = []
    if str(summary.get("status", "")).upper() == "FAILED" or not _is_blank(summary.get("error_message")):
        error = _format_cell("error_message", summary.get("error_message"))
        issues.append(_humanize_top_error(error if error != "-" else "流程執行失敗"))
    if (_to_float(summary.get("market_intel_warning_count")) or 0) > 0:
        issues.append("市場情報資料不足，未影響流程")
    if str(summary.get("market_intel_status", "")).upper() == "CACHE":
        issues.append("市場情報使用快取資料")
    if not data_fetch_status.empty and "status" in data_fetch_status.columns:
        for _, row in data_fetch_status.iterrows():
            issue = _data_source_quality_issue(row)
            if issue and issue not in issues:
                issues.append(issue)
    return "；".join(issues) if issues else "無重大錯誤"


def _data_source_quality_issue(row: pd.Series) -> str:
    source = str(row.get("source_name", "")).strip()
    status = str(row.get("status", "")).strip().upper()
    fallback_action = str(row.get("fallback_action", "")).strip()
    warning = str(row.get("warning", "")).strip()
    error_message = str(row.get("error_message", "")).strip()
    if source == "monthly_revenue" and ("HTTPError: 404" in error_message or "404 Client Error" in error_message):
        return "月營收資料尚未取得，已保留既有資料，不影響今日流程"
    if status == "OK_WITH_FALLBACK" or fallback_action == "kept_existing_csv":
        return _monthly_revenue_fallback_text(source) if source == "monthly_revenue" else "部分資料來源已保留既有資料"
    if status in {"FAILED", "MISSING"}:
        return _monthly_revenue_fallback_text(source) if source == "monthly_revenue" else "部分資料來源失敗，已 fallback"
    if status == "EMPTY":
        return "部分資料來源為空，採中性或既有資料"
    if status == "CACHE":
        return "部分資料來源使用快取資料"
    if "kept existing csv" in warning:
        return _monthly_revenue_fallback_text(source) if source == "monthly_revenue" else "部分資料來源已保留既有資料"
    return ""


def _monthly_revenue_fallback_text(source: str) -> str:
    if source == "monthly_revenue":
        return "月營收資料尚未取得，已保留既有資料，不影響今日流程"
    return "部分資料來源已保留既有資料"


def _humanize_top_error(message: str) -> str:
    if "mops.twse.com.tw" in message or "HTTPError" in message:
        return "資料來源暫不可用，已使用 fallback 或既有資料"
    return message


def _health_checks(
    report_dir: Path,
    summary: dict[str, object],
    candidates: pd.DataFrame,
    risk_pass: pd.DataFrame,
    pending_orders: pd.DataFrame,
    paper_trades: pd.DataFrame,
    market_intel: pd.DataFrame,
    data_fetch_status: pd.DataFrame,
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
    items.extend(
        [
            (
                "data update",
                "警告" if summary and summary.get("status") == "FAILED" and summary.get("error_step") == "run_daily" else "正常",
                _format_cell("status", summary.get("status")) if summary else "缺少 daily summary",
            ),
            (
                "candidate export",
                "注意" if candidates.empty else "正常",
                "今日無候選股資料" if candidates.empty else f"{len(candidates):,.0f} 筆",
            ),
            (
                "paper trade",
                "正常" if list(report_dir.glob("pending_orders_*.csv")) or not pending_orders.empty else "注意",
                "已檢查 pending order 檔案" if list(report_dir.glob("pending_orders_*.csv")) or not pending_orders.empty else "尚無 pending order 檔案",
            ),
            (
                "position update",
                "注意" if paper_trades.empty else "正常",
                "目前尚無紙上交易紀錄" if paper_trades.empty else f"{len(paper_trades):,.0f} 筆",
            ),
            (
                "market intelligence",
                "注意" if market_intel.empty else "正常",
                "市場判斷資料不足" if market_intel.empty else f"{len(market_intel):,.0f} 筆",
            ),
            (
                "report generation",
                "正常",
                "reports/index.html 已產生",
            ),
            (
                "Discord notification",
                "注意",
                "GitHub Actions 執行時才可確認 webhook 結果",
            ),
        ]
    )
    stale = _stale_pending_count(pending_orders, trade_date)
    items.append(
        (
            "pending order 超過 3 天仍未成交",
            "警告" if stale > 0 else "正常",
            f"{stale} 筆",
        )
    )
    items.extend(_data_source_health_items(data_fetch_status))
    return items


def _data_source_health_items(data_fetch_status: pd.DataFrame) -> list[tuple[str, str, str]]:
    if data_fetch_status.empty:
        return [("資料來源狀態", "注意", "找不到最新 data_fetch_status_*.csv")]

    items: list[tuple[str, str, str]] = []
    for _, row in data_fetch_status.iterrows():
        source = _format_cell("source_name", row.get("source_name"))
        status_text = str(row.get("status", "")).strip().upper()
        rows = int(_to_float(row.get("rows")) or 0)
        maturity = str(row.get("provider_maturity", "")).strip()
        fallback_action = str(row.get("fallback_action", "")).strip()
        warning = _data_source_warning_text(row)
        error_message = str(row.get("error_message", "")).strip()
        health_status = _provider_health_status(status_text, rows, maturity)
        detail_parts = [
            f"狀態：{_format_data_source_status(status_text)}",
            f"筆數：{rows:,.0f}",
        ]
        if maturity:
            detail_parts.append(f"成熟度：{maturity}")
        if fallback_action:
            detail_parts.append(f"fallback：{fallback_action}")
        if warning:
            detail_parts.append(f"警告：{warning[:160]}")
        if error_message:
            detail_parts.append(f"錯誤：{error_message[:160]}")
        items.append((f"資料來源：{source}", health_status, "；".join(detail_parts)))
    return items


def _data_source_warning_text(row: pd.Series) -> str:
    source = str(row.get("source_name", "")).strip()
    error_message = str(row.get("error_message", "")).strip()
    if source == "monthly_revenue" and ("HTTPError: 404" in error_message or "404 Client Error" in error_message):
        return "月營收資料尚未發布或來源暫不可用，已保留既有資料。"
    return str(row.get("warning", "")).strip()


def _format_data_source_status(status_text: str) -> str:
    return {
        "OK": "正常",
        "OK_WITH_FALLBACK": "成功，保留既有資料",
        "CACHE": "使用快取資料",
        "EMPTY": "無資料",
        "FAILED": "失敗",
        "MISSING": "資料缺失",
    }.get(str(status_text).strip().upper(), status_text)


def _provider_health_status(status_text: str, rows: int, maturity: str) -> str:
    maturity_text = str(maturity).strip().lower()
    status = str(status_text).strip().upper()
    if status in {"FAILED", "MISSING"}:
        return "警告"
    if status in {"CACHE", "EMPTY", "OK_WITH_FALLBACK"}:
        return "注意"
    if status == "OK" and rows == 0:
        return "注意"
    if maturity_text in {"placeholder", "csv_fallback"}:
        return "注意"
    return "正常"


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
        ("正常資料源數", f"{normal_count:,.0f}"),
        ("注意資料源數", f"{attention_count:,.0f}"),
        ("警告資料源數", f"{warning_count:,.0f}"),
    ]
    return '<div class="cards health-summary">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _warning_banner(items: list[tuple[str, str, str]]) -> str:
    warnings = [_top_warning_message(name, detail) for name, status, detail in items if status == "警告"]
    notices = [
        "月營收資料尚未取得，已保留既有資料，不影響今日流程。"
        for name, status, detail in items
        if status == "注意" and "資料來源：monthly_revenue" in name and "已保留既有資料" in detail
    ]
    messages = warnings + list(dict.fromkeys(notices))
    if not messages:
        return ""
    return '<div class="top-warning"><strong>注意</strong><span>' + escape("；".join(messages)) + "</span></div>"


def _top_warning_message(name: str, detail: str) -> str:
    if "資料來源：monthly_revenue" in name and ("HTTPError: 404" in detail or "404 Client Error" in detail):
        return "月營收資料尚未取得，已保留既有資料，不影響今日流程。"
    return f"{name}：{_strip_urls(detail)}"


def _strip_urls(text: str) -> str:
    return re.sub(r"https?://\S+", "[URL 已隱藏]", text)


def _stale_pending_count(pending_orders: pd.DataFrame, trade_date: pd.Timestamp) -> int:
    if pending_orders.empty or "status" not in pending_orders.columns or pd.isna(trade_date):
        return 0
    frame = pending_orders[pending_orders["status"].fillna("").astype(str) == "PENDING"].copy()
    if frame.empty or "signal_date" not in frame.columns:
        return 0
    signal_dates = pd.to_datetime(frame["signal_date"], errors="coerce")
    return int(((trade_date - signal_dates).dt.days > 3).fillna(False).sum())


def _data_confidence_summary(
    candidates: pd.DataFrame,
    market_intel: pd.DataFrame,
    summary: dict[str, object],
    data_fetch_status: pd.DataFrame,
) -> str:
    frame = market_intel if not market_intel.empty else candidates
    source = _market_intel_source(summary, frame)
    is_mock = source.lower() == "mock"
    using_cache = (
        str(summary.get("market_intel_status", "")).upper() == "CACHE"
        or (not data_fetch_status.empty and "status" in data_fetch_status.columns and data_fetch_status["status"].fillna("").astype(str).str.upper().eq("CACHE").any())
    )
    cards = [
        ("市場情報來源", source or "-"),
        ("是否為 mock", "是" if is_mock else "否"),
        ("是否使用 cache", "是" if using_cache else "否"),
        ("市場情報資料不足股票數", _format_cell("market_intel_warning_count", summary.get("market_intel_warning_count"))),
        ("基本面資料不足股票數", f"{_fundamental_missing_count(candidates):,.0f}"),
        ("估值資料不足股票數", f"{_reason_missing_count(candidates, 'valuation_score', 'valuation_reason'):,.0f}"),
        ("財報資料不足股票數", f"{_reason_missing_count(candidates, 'financial_score', 'financial_reason'):,.0f}"),
        ("月營收資料狀態", _source_status_summary(data_fetch_status, "monthly_revenue")),
        ("三大法人資料狀態", _source_status_summary(data_fetch_status, "institutional")),
        ("融資融券資料狀態", _source_status_summary(data_fetch_status, "margin_short")),
        ("注意 / 處置股資料狀態", _source_status_summary(data_fetch_status, "attention_disposition")),
    ]
    notes = []
    if is_mock:
        notes.append("目前為 mock / 中性資料，尚未接入正式新聞來源，不應視為完整新聞 / 財報分析。")
        notes.append("新聞來源狀態：尚未接入")
    if _fundamental_missing_is_majority(candidates):
        notes.append("目前基本面資料完整度不足，多數股票使用中性分數 50，請勿視為完整財報分析。")
    notes.append(
        "分數用途說明：total_score 是技術面原始候選分數；multi_factor_score 是多因子輔助分；"
        "final_market_score 是市場情報綜合分，目前不直接影響下單；confidence_score 是資料可信度，低於 60 通常代表資料不足。"
    )
    return _section(
        "資料可信度總覽",
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + "".join(f'<div class="note">{escape(note)}</div>' for note in notes),
        class_name="data-confidence-summary",
    )


def _market_intel_source(summary: dict[str, object], frame: pd.DataFrame) -> str:
    if not _is_blank(summary.get("market_intel_source")):
        return str(summary.get("market_intel_source")).strip()
    if not frame.empty and "market_intel_source" in frame.columns:
        values = [str(value).strip() for value in frame["market_intel_source"] if not _is_blank(value)]
        if values:
            return values[0]
    return "-"


def _source_status_summary(data_fetch_status: pd.DataFrame, source_name: str) -> str:
    if data_fetch_status.empty or "source_name" not in data_fetch_status.columns:
        return "無紀錄"
    matches = data_fetch_status[data_fetch_status["source_name"].fillna("").astype(str) == source_name]
    if matches.empty:
        return "無紀錄"
    row = matches.iloc[0]
    status = _format_cell("market_intel_status", row.get("status"))
    rows = int(_to_float(row.get("rows")) or 0)
    maturity = str(row.get("provider_maturity", "")).strip()
    fallback = str(row.get("fallback_action", "")).strip()
    parts = [status, f"{rows:,.0f} 筆"]
    if maturity:
        parts.append(maturity)
    if fallback:
        parts.append(fallback)
    return " / ".join(parts)


def _fundamental_missing_count(candidates: pd.DataFrame) -> int:
    return _reason_missing_count(candidates, "fundamental_score", "fundamental_reason")


def _reason_missing_count(frame: pd.DataFrame, score_column: str, reason_column: str) -> int:
    if frame.empty:
        return 0
    scores = (
        pd.to_numeric(frame[score_column], errors="coerce").fillna(50)
        if score_column in frame.columns
        else pd.Series([50] * len(frame), index=frame.index)
    )
    reasons = (
        frame[reason_column].fillna("").astype(str)
        if reason_column in frame.columns
        else pd.Series([""] * len(frame), index=frame.index)
    )
    return int(((scores == 50) & reasons.str.contains("資料不足", na=False)).sum())


def _fundamental_missing_is_majority(candidates: pd.DataFrame) -> bool:
    return not candidates.empty and _fundamental_missing_count(candidates) >= max(1, len(candidates) // 2 + len(candidates) % 2)


def _market_intel_summary(
    candidates: pd.DataFrame,
    market_intel: pd.DataFrame,
    summary: dict[str, object],
) -> str:
    frame = market_intel if not market_intel.empty else candidates
    if frame.empty:
        return _section("市場判斷摘要", _empty("今日無市場判斷資料"), class_name="market-intel-summary")
    warning_count = _count_non_empty(frame, "market_intel_warning")
    source = _market_intel_source(summary, frame)
    is_mock = source.lower() == "mock"
    negative_news = 0
    if not is_mock and "news_sentiment_score" in frame.columns:
        negative_news = int((pd.to_numeric(frame["news_sentiment_score"], errors="coerce").fillna(0) < 0).sum())
    top_score = _format_cell("final_market_score", summary.get("market_intel_top_score"))
    cards = [
        ("市場判斷狀態", _format_cell("market_intel_status", summary.get("market_intel_status"))),
        ("市場判斷來源", source),
        ("市場判斷最高分", top_score),
        ("市場情報資料不足股票數", f"{warning_count:,.0f}"),
        ("新聞來源狀態", "尚未接入" if is_mock else "已接入或可用"),
        ("注意股候選數", f"{_count_true(frame, 'is_attention_stock'):,.0f}"),
        ("處置股候選數", f"{_count_true(frame, 'is_disposition_stock'):,.0f}"),
        ("被阻擋候選數", f"{_count_true(frame, 'event_blocked'):,.0f}"),
    ]
    if not is_mock:
        cards.append(("新聞偏負面候選", f"{negative_news:,.0f}"))
    columns = [
        "stock_id",
        "stock_name",
        "market_fundamental_score",
        "market_valuation_score",
        "market_momentum_score",
        "market_chip_score",
        "institutional_score",
        "credit_score",
        "event_risk_score",
        "liquidity_score",
        "sector_strength_score",
        "news_sentiment_score",
        "final_market_score",
        "confidence_score",
        "risk_flags",
        "is_attention_stock",
        "attention_reason",
        "is_disposition_stock",
        "disposition_reason",
        "event_reason",
        "event_blocked",
        "final_comment",
        "data_source_warning",
        "system_comment",
        "market_intel_warning",
    ]
    detail = _responsive_records(frame, columns, "今日無市場判斷資料", 20)
    note = ""
    if is_mock:
        note = '<div class="note">目前為 mock / 中性資料，尚未接入正式新聞來源，不應視為完整新聞 / 財報分析。</div>'
    return _section(
        "市場判斷摘要",
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + note
        + _details_block("市場判斷候選股明細", detail),
        class_name="market-intel-summary",
    )


def _multi_factor_summary(candidates: pd.DataFrame, summary: dict[str, object]) -> str:
    if candidates.empty:
        return _empty("目前尚無多因子資料")
    high_risk = _count_true(candidates, "event_blocked")
    valuation_warning = _count_non_empty(candidates, "valuation_warning")
    financial_warning = _count_non_empty(candidates, "financial_warning")
    institutional_positive = _count_score_above(candidates, "institutional_score", 50)
    cards = [
        ("多因子資料更新狀態", _format_cell("multi_factor_data_status", summary.get("multi_factor_data_status"))),
        ("高風險事件警告數", f"{high_risk:,.0f}"),
        ("基本面加分候選股數", f"{_count_score_above(candidates, 'revenue_score', 50):,.0f}"),
        ("估值警告候選股數", f"{valuation_warning:,.0f}"),
        ("財報警告候選股數", f"{financial_warning:,.0f}"),
        ("籌碼加分候選股數", f"{institutional_positive:,.0f}"),
    ]
    return '<h3>多因子分數摘要</h3><div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _fundamental_summary(candidates: pd.DataFrame) -> str:
    if candidates.empty:
        return _empty("基本面資料不足，採中性分數")
    fundamental_values = (
        pd.to_numeric(candidates["fundamental_score"], errors="coerce").fillna(50)
        if "fundamental_score" in candidates.columns
        else pd.Series([50] * len(candidates))
    )
    positive = int((fundamental_values > 50).sum())
    warning = int((fundamental_values < 50).sum())
    missing = _fundamental_missing_count(candidates)
    cards = [
        ("基本面加分候選股數", f"{positive:,.0f}"),
        ("基本面警告候選股數", f"{warning:,.0f}"),
        ("基本面資料不足股票數", f"{missing:,.0f}"),
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
        + (
            '<div class="note">目前基本面資料完整度不足，多數股票使用中性分數 50，請勿視為完整財報分析。</div>'
            if _fundamental_missing_is_majority(candidates)
            else ""
        )
        + _details_block("基本面候選股詳細表", table)
    )


def _count_true(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].apply(lambda value: str(value).strip().lower() in {"true", "1", "yes", "y"}).sum())


def _count_non_empty(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int((frame[column].fillna("").astype(str).str.strip() != "").sum())


def _count_score_above(frame: pd.DataFrame, column: str, threshold: float) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int((pd.to_numeric(frame[column], errors="coerce").fillna(50) > threshold).sum())


def _exit_strategy_summary(
    summary: dict[str, object],
    open_positions: pd.DataFrame,
    closed_trades: pd.DataFrame,
) -> str:
    prefix = "最近有效交易日" if _uses_recent_data(summary) else "今日"
    cards = [
        (f"{prefix}停利筆數", _format_cell("take_profit_exits", summary.get("take_profit_exits"))),
        (f"{prefix}停損筆數", _format_cell("stop_loss_exits", summary.get("stop_loss_exits"))),
        (f"{prefix}移動停利筆數", _format_cell("trailing_stop_exits", summary.get("trailing_stop_exits"))),
        (f"{prefix}趨勢出場筆數", _format_cell("trend_exit_exits", summary.get("trend_exit_exits"))),
        (f"{prefix}扣成本後已實現損益", _format_cell("realized_pnl_after_cost_today", summary.get("realized_pnl_after_cost_today"))),
    ]
    open_display = open_positions.copy()
    if not open_display.empty and "exit_reason" in open_display.columns:
        open_display["recent_partial_exit_reason"] = open_display["exit_reason"]
    open_table = _table(
        open_display,
        [
            "stock_id",
            "stock_name",
            "partial_exit_1_done",
            "remaining_shares",
            "highest_price_since_entry",
            "trailing_stop_price",
            "recent_partial_exit_reason",
        ],
        "目前尚無出場策略持倉資料",
        max_rows=50,
    )
    return (
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + _details_block("出場策略持倉明細", open_table)
    )


def _paper_performance(summary: dict[str, object], closed_trades: pd.DataFrame, open_positions: pd.DataFrame) -> str:
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

    today_exits = _today_exit_frame(closed_trades, open_positions, summary.get("trade_date") if summary else None)
    blocks.append(
        _details_block(
            "今日出場明細",
            _table(
                today_exits,
                [
                    "stock_id",
                    "stock_name",
                    "exit_type",
                    "exit_date",
                    "exit_reason",
                    "exit_price",
                    "realized_pnl_after_cost",
                    "realized_pnl_pct_after_cost",
                    "total_cost",
                    "status",
                ],
                "今日尚無出場交易",
                max_rows=50,
            ),
        )
    )
    blocks.append(
        _details_block(
            "累計已平倉交易明細",
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


def _today_exit_frame(
    closed_trades: pd.DataFrame,
    open_positions_or_trade_date: pd.DataFrame | object | None = None,
    trade_date: object | None = None,
) -> pd.DataFrame:
    if trade_date is None:
        if isinstance(open_positions_or_trade_date, pd.DataFrame):
            open_positions = open_positions_or_trade_date
            trade_date = None
        else:
            open_positions = pd.DataFrame()
            trade_date = open_positions_or_trade_date
    else:
        open_positions = open_positions_or_trade_date if isinstance(open_positions_or_trade_date, pd.DataFrame) else pd.DataFrame()
    target = _normalized_date_text(trade_date)
    if not target:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    if not closed_trades.empty and "exit_date" in closed_trades.columns:
        closed = closed_trades.copy()
        closed = closed[closed["exit_date"].apply(_normalized_date_text) == target].copy()
        if not closed.empty:
            closed["exit_type"] = "完整出場"
            frames.append(closed)
    if not open_positions.empty and {"exit_date", "exit_reason"}.issubset(open_positions.columns):
        open_frame = open_positions.copy()
        reasons = open_frame["exit_reason"].fillna("").astype(str).str.upper()
        open_frame = open_frame[
            (open_frame["exit_date"].apply(_normalized_date_text) == target)
            & reasons.isin({"TAKE_PROFIT_1", "TAKE_PROFIT_2"})
        ].copy()
        if not open_frame.empty:
            open_frame["exit_type"] = "部分停利 / 部分出場"
            if "exit_price" not in open_frame.columns:
                open_frame["exit_price"] = ""
            open_frame["exit_price"] = open_frame["exit_price"].where(
                ~open_frame["exit_price"].apply(_is_blank),
                "部分出場紀錄",
            )
            frames.append(open_frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


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

    if fallback != "-" and _uses_recent_data(summary):
        return (
            '<div class="note">'
            f"今日無交易資料，已使用最近有效交易日。原始執行日期：{escape(requested)}；"
            f"實際交易日：{escape(actual)}；使用替代交易日：{escape(fallback)}；"
            f"替代原因：{escape(reason)}；狀態：{escape(status)}。"
            "</div>"
        )
    return '<div class="note">本次使用原始交易日資料，未切換至替代交易日。</div>'


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
.positive{color:#f87171!important}
.negative{color:#34d399!important}
.neutral{color:#e5e7eb!important}
.broker-cards{display:grid;gap:12px}
.mobile-cards{display:grid;gap:12px}
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
@media(min-width:760px){.page{padding:22px}.account-header h1{font-size:32px}.section-tabs{margin:12px 0 16px;padding:10px 0}.pnl-primary{grid-template-columns:repeat(4,minmax(0,1fr))}.pnl-secondary,.cards{grid-template-columns:repeat(auto-fit,minmax(160px,1fr))}.holding-main{grid-template-columns:220px 1fr}.broker-cards,.mobile-cards{grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}.health-grid{grid-template-columns:repeat(auto-fit,minmax(220px,1fr))}}
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
