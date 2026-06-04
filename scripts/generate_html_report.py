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
from tw_quant.reporting.position_review import generate_position_review_summary
from tw_quant.reporting.data_quality import write_data_quality_health


COLUMN_LABELS = {
    "rank": "排序",
    "trade_date": "實際交易日",
    "requested_date": "原始執行日期",
    "fallback_date": "使用替代交易日",
    "fallback_reason": "替代原因",
    "actual_data_date": "實際資料日",
    "cache_age_days": "快取 / 資料年齡天數",
    "is_stale_data": "是否過期資料",
    "data_freshness_level": "資料鮮度等級",
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
    "data_quality_flags": "資料不足旗標",
    "investment_risk_flags": "投資風險旗標",
    "final_comment": "系統短評",
    "error_step": "失敗步驟",
    "error_message": "錯誤訊息",
    "market_regime_score": "市場環境分數",
    "new_entries_allowed": "是否允許新增持倉",
    "guardrail_status": "Guardrail 狀態",
    "pause_new_entries_reason": "暫停新倉原因",
    "rejected_orders": "被擋下交易數",
    "pending_orders_active_count": "Active pending 數",
    "pending_orders_executed_count": "Executed pending 數",
    "pending_orders_expired_count": "Expired pending 數",
    "pending_orders_cancelled_count": "Cancelled pending 數",
    "rejected_orders_signal_count": "訊號建立被擋數",
    "rejected_orders_execution_count": "執行前被擋數",
    "rejected_orders_total_count": "被擋總數",
    "guardrail_blocked_execution_count": "Guardrail 執行擋單數",
    "expired_pending_orders_count": "過期待進場數",
    "cancelled_by_market_regime_count": "市場環境取消數",
    "cancelled_by_low_grade_count": "分級不足取消數",
    "cancelled_by_event_risk_count": "事件風險取消數",
    "cancelled_by_max_position_count": "持倉上限取消數",
    "loss_attribution_status": "虧損歸因狀態",
    "loss_attribution_loss_count": "虧損交易數",
    "loss_attribution_top_reason": "主要虧損原因",
    "gap_pct": "跳空 / 進場落差",
    "max_favorable_excursion": "最大有利波動",
    "max_adverse_excursion": "最大不利波動",
    "loss_bucket": "虧損分類",
    "likely_loss_reason": "可能虧損原因",
    "rejected_reason": "拒絕建立原因",
    "rejection_stage": "拒絕階段",
    "rejection_reason": "拒絕 / 取消原因",
    "rejected_status": "拒絕狀態",
    "original_order_status": "原始訂單狀態",
    "final_order_status": "最終訂單狀態",
    "attempted_execution_date": "嘗試執行日",
    "order_age_trading_days": "訂單年齡（交易日）",
    "expires_after_trading_days": "有效期限（交易日）",
    "expired_at": "過期日",
    "expiry_reason": "過期原因",
    "ai_enrichment_status": "AI / Enrichment 狀態",
    "ai_used_count": "AI 使用筆數",
    "rule_based_enrichment_count": "Rule-based fallback 筆數",
    "enrichment_insufficient_data_count": "資料不足筆數",
    "industry_map_status": "產業分類補強狀態",
    "pnl_chart_status": "今日損益圖狀態",
    "market_recap_status": "大盤復盤狀態",
    "decision_dashboard_status": "決策儀表盤狀態",
    "config_summary_status": "配置說明狀態",
    "enrichment_evidence_status": "資料來源依據狀態",
    "enrichment_status": "資料補強狀態",
    "enrichment_provider": "資料補強 provider",
    "ai_used": "是否使用外部 AI",
    "source_evidence_count": "資料來源依據數",
    "missing_data_flags": "缺失資料旗標",
    "enriched_industry": "補強產業",
    "enriched_industry_source": "產業資料來源",
    "valuation_context": "估值脈絡",
    "valuation_risk_level": "估值風險等級",
    "margin_credit_context": "融資籌碼脈絡",
    "margin_risk_level": "融資風險等級",
    "sector_context": "產業脈絡",
    "risk_explanation": "風險解釋",
    "opportunity_explanation": "觀察重點",
    "data_quality_explanation": "資料品質說明",
    "manual_review_focus": "人工檢查重點",
    "ai_summary": "AI / Enrichment 摘要",
    "ai_warning": "AI / Enrichment 警示",
    "source_evidence_json": "資料來源依據",
    "industry_main": "主要產業",
    "industry_sub": "細分產業",
    "sector_strength_mode": "相對強弱模式",
    "margin_price_divergence": "融資價格背離",
    "market_recap_status": "大盤復盤狀態",
    "regime_label": "市場狀態",
    "twse_index": "加權指數",
    "tpex_index": "櫃買指數",
    "advancers": "上漲家數",
    "decliners": "下跌家數",
    "unchanged": "平盤家數",
    "limit_up_count": "漲停家數",
    "limit_down_count": "跌停家數",
    "market_breadth_summary": "市場廣度摘要",
    "recap_summary": "復盤摘要",
    "fallback_used": "是否使用 fallback",
    "data_quality_note": "資料品質說明",
    "today_total_pnl": "今日總損益",
    "total_return_pct": "報酬率",
    "source_name": "資料源",
    "source_type": "來源類型",
    "source_date": "來源日期",
    "field_name": "欄位",
    "field_value": "欄位值",
    "evidence_summary": "依據摘要",
    "confidence_impact": "可信度影響",
}


MARKET_RECAP_COLUMNS_FOR_TABLE = [
    "trade_date",
    "market_regime_score",
    "regime_label",
    "twse_index",
    "tpex_index",
    "advancers",
    "decliners",
    "unchanged",
    "limit_up_count",
    "limit_down_count",
    "market_breadth_summary",
    "recap_summary",
    "fallback_used",
    "data_quality_note",
]


COLUMN_LABELS.update(
    {
        "summary_requested_date": "執行日",
        "summary_trade_date": "交易日",
        "summary_status": "狀態",
        "summary_candidate_rows": "候選",
        "summary_risk_pass_rows": "風控通過",
        "summary_pending_orders": "待進場",
        "summary_executed_orders": "成交",
        "summary_open_positions": "持倉",
        "summary_closed_positions": "已出場",
        "summary_total_equity_after_cost": "帳戶總資產",
        "summary_data_status": "資料狀態",
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
        "sector_strength_mode": "相對強弱模式",
        "sector_strength_reason": "產業強弱理由",
        "sector_strength_warning": "產業強弱警告",
        "latest_relative_mode": "最新相對強弱模式",
        "fallback_reason": "Fallback 原因",
        "appear_in_candidates_count": "候選股出現次數",
        "appear_in_trading_decisions_count": "決策表出現次數",
        "appear_in_risk_pass_count": "風控通過出現次數",
        "appear_in_position_review_count": "持倉檢查出現次數",
        "appear_in_ai_enrichment_count": "Enrichment 出現次數",
        "recent_appearance_count": "近期出現總次數",
        "avg_volume": "平均成交量",
        "turnover_value": "成交金額",
        "last_seen_date": "最後出現日期",
        "priority_score": "優先分數",
        "priority_level": "優先等級",
        "suggested_action": "建議動作",
        "query": "查詢字串",
        "proposed_market_type": "候選市場類型",
        "proposed_industry": "候選產業",
        "proposed_sub_industry": "候選子產業",
        "source_title": "來源標題",
        "source_url": "來源網址",
        "confidence": "可信度",
        "checked_at": "查詢時間",
        "avg_volume_20d": "20 日均量",
        "avg_turnover_20d": "20 日均成交金額",
        "latest_volume": "最新成交量",
        "latest_turnover": "最新成交金額",
        "turnover_ratio_20d": "量能 / 20 日均量比",
        "intraday_trading_ratio": "當日量能倍數",
        "liquidity_warning": "流動性警告",
        "slippage_risk_score": "滑價風險分數",
        "risk_light": "持倉風險燈號",
        "holding_action_hint": "持倉提示",
    "holding_risk_reason": "燈號原因",
    "strategy_validation_status": "策略驗證狀態",
    "trading_decisions_status": "決策引擎狀態",
    "buy_candidate_count": "買進候選數",
    "watch_only_count": "觀察名單數",
    "no_trade_count": "不交易名單數",
    "hold_count": "持倉 HOLD 數",
    "reduce_count": "REDUCE review 數",
    "exit_review_count": "EXIT review 數",
    "grade_a_count": "A 級候選股數",
    "grade_b_count": "B 級候選股數",
    "grade_c_count": "C 級候選股數",
    "grade_d_count": "D 級候選股數",
    "decision_date": "決策日期",
    "source": "來源",
    "current_status": "目前狀態",
    "decision": "決策",
    "decision_level": "決策層級",
    "action": "動作",
    "candidate_grade": "候選分級",
    "grade_reason": "分級理由",
    "grade_risk_flags": "分級風險標籤",
    "requires_manual_review": "需要人工確認",
    "review_level": "檢查層級",
    "review_reason": "檢查原因",
    "position_size_suggestion": "部位提示",
    "can_auto_trade": "可否自動交易",
    "data_quality_note": "資料品質註記",
    "validation_date": "驗證日期",
    "model_name": "模型名稱",
    "description": "說明",
    "selected_count": "選取數",
    "simulated_trades": "模擬交易數",
    "win_rate": "勝率",
    "avg_return_pct": "平均報酬",
    "median_return_pct": "中位報酬",
    "total_return_pct": "總報酬",
    "max_drawdown_pct": "最大回撤",
    "avg_holding_days": "平均持有天數",
    "profit_factor": "獲利因子",
    "expectancy": "期望值",
    "consecutive_loss_count": "連續虧損數",
    "notes": "備註",
    }
)

COLUMN_LABELS.update(
    {
        "revenue_data_month": "月營收資料月份",
        "requested_revenue_month": "原始月營收月份",
        "latest_available_month": "最近可用月營收月份",
        "revenue_source_status": "月營收來源狀態",
        "valuation_source": "估值資料來源",
        "valuation_source_status": "估值來源狀態",
        "financial_source": "財報資料來源",
        "financial_source_status": "財報來源狀態",
        "financial_period": "財報期間",
        "requested_period": "要求期間",
        "actual_period": "實際期間",
        "latest_available_period": "最近可用期間",
        "source_url_or_name": "來源名稱",
        "source_name": "資料源",
        "fallback_action": "Fallback 動作",
        "is_real_data": "是否真實資料",
        "is_mock": "是否 mock",
        "is_stale": "是否過舊",
        "data_age_days": "資料年齡天數",
        "data_freshness_level": "資料鮮度等級",
        "coverage_ratio": "覆蓋率",
        "affected_symbols_count": "影響股票數",
        "check_name": "檢查項目",
        "category": "類別",
        "health_status": "健康狀態",
        "data_issue": "資料問題",
        "investment_risk": "投資風險",
        "is_stale_data": "是否過期資料",
        "positive_signals": "正向訊號",
        "warning_signals": "警示訊號",
        "blocking_risks": "阻擋風險",
        "momentum_signal": "動能訊號",
        "market_type": "市場類型",
        "tracking_index": "追蹤指數",
        "fund_size": "基金規模",
        "expense_ratio": "總費用率",
        "discount_premium": "折溢價",
        "top_holdings_available": "是否有前十大持股",
        "etf_data_quality_flags": "ETF 資料品質旗標",
        "is_etf": "是否 ETF",
        "has_industry": "有產業分類",
        "has_valuation": "有估值資料",
        "has_financials": "有財報資料",
        "has_revenue": "有營收資料",
        "has_institutional": "有法人資料",
        "has_margin": "有融資融券資料",
        "has_event_data": "有事件資料",
        "has_etf_metadata": "有 ETF metadata",
        "missing_fields": "缺失欄位",
        "hold": "持倉續抱",
        "reduce": "降低風險檢查",
        "exit_review": "出場檢查",
        "near_stop_loss": "接近停損",
        "near_take_profit": "接近停利",
        "data_quality_warning": "資料品質警示",
    }
)

STATUS_LABELS = {
    "OK": "成功",
    "OK_WITH_FALLBACK": "成功，使用最近有效交易日",
    "OK_WITH_WARNING": "成功但有資料警告",
    "CACHE": "使用快取資料",
    "CURRENT": "目前最新交易日資料",
    "RECENT": "近一個交易日內",
    "STALE": "資料過期",
    "UNKNOWN": "無法判斷",
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
    "EXPIRED": "已過期",
    "CANCELLED_BY_GUARDRAIL": "Guardrail 取消",
    "CANCELLED_BY_MARKET_REGIME": "市場環境取消",
    "CANCELLED_BY_MAX_POSITION": "持倉上限取消",
    "CANCELLED_BY_LOW_GRADE": "分級不足取消",
    "CANCELLED_BY_EVENT_RISK": "事件風險取消",
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
    "BUY_CANDIDATE": "買進候選，需人工確認",
    "HOLD": "持倉檢查",
    "REDUCE": "降低風險檢查",
    "EXIT": "出場訊號檢查",
    "NO_TRADE": "不交易",
    "WATCH_ONLY": "觀察名單",
    "INFO": "資訊",
    "WATCH": "觀察",
    "CAUTION": "注意",
    "HIGH_RISK": "高風險",
    "observe_only": "僅觀察",
    "review_before_entry": "進場前人工確認",
    "review_holding": "持倉檢查",
    "reduce_risk": "降低風險檢查",
    "exit_signal_review": "出場訊號檢查",
    "no_action": "不動作",
    "True": "是",
    "False": "否",
    "BLOCKED": "暫停新增持倉",
    "REJECTED_GUARDRAIL": "Guardrail 擋下",
    "profitable_or_flat": "獲利或持平",
    "large_loss": "大額虧損",
    "small_loss": "小額虧損",
    "unrealized_loss": "未實現虧損",
    "STANDARD_REVIEW": "一般檢查",
    "DATA_REVIEW": "資料檢查",
    "RISK_REVIEW": "風險檢查",
    "WARNING": "警告",
    "ATTENTION": "注意",
}

STATUS_LABELS.update({"MOCK": "mock / 中性資料"})

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
    "today_total_pnl",
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
    "summary_candidate_rows",
    "summary_risk_pass_rows",
    "summary_pending_orders",
    "summary_executed_orders",
    "summary_open_positions",
    "summary_closed_positions",
    "rows",
    "affected_symbols_count",
    "cache_age_days",
    "data_age_days",
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
    "summary_status",
    "summary_data_status",
    "strategy_validation_status",
    "trading_decisions_status",
    "industry_map_status",
    "decision",
    "decision_level",
    "action",
    "can_auto_trade",
    "requires_manual_review",
    "review_level",
    "health_status",
    "data_issue",
    "investment_risk",
    "is_stale_data",
    "data_freshness_level",
}
DATE_COLUMNS = {
    "trade_date",
    "requested_date",
    "fallback_date",
    "exit_date",
    "signal_date",
    "planned_entry_date",
    "actual_entry_date",
    "summary_requested_date",
    "summary_trade_date",
    "decision_date",
    "validation_date",
    "actual_data_date",
}
AMOUNT_COLUMNS.add("summary_total_equity_after_cost")
SCORE_COLUMNS.update(
    {
        "market_chip_score",
        "credit_score",
        "event_risk_score",
        "liquidity_score",
        "sector_strength_score",
        "slippage_risk_score",
        "win_rate",
        "avg_return_pct",
        "median_return_pct",
        "total_return_pct",
        "max_drawdown_pct",
        "profit_factor",
        "expectancy",
        "market_regime_score",
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
        "turnover_ratio_20d",
        "win_rate",
        "avg_return_pct",
        "median_return_pct",
        "total_return_pct",
        "max_drawdown_pct",
        "gap_pct",
        "max_favorable_excursion",
        "max_adverse_excursion",
    }
)
AMOUNT_COLUMNS.update({"monthly_revenue", "avg_turnover_20d", "latest_turnover"})
AMOUNT_COLUMNS.update({"fund_size"})
PERCENT_COLUMNS.update({"expense_ratio", "discount_premium"})
INTEGER_COLUMNS.update(
    {
        "latest_volume",
        "foreign_buy_days",
        "investment_trust_buy_days",
        "margin_balance",
        "margin_change",
        "short_balance",
        "short_change",
        "securities_lending_sell_volume",
        "securities_lending_balance",
        "avg_volume_20d",
        "buy_candidate_count",
        "watch_only_count",
        "no_trade_count",
        "hold_count",
        "reduce_count",
        "exit_review_count",
        "grade_a_count",
        "grade_b_count",
        "grade_c_count",
        "grade_d_count",
        "candidate_count",
        "selected_count",
        "simulated_trades",
        "stop_loss_count",
        "take_profit_count",
        "trailing_stop_count",
        "ma_exit_count",
        "max_holding_exit_count",
        "consecutive_loss_count",
        "rejected_orders",
        "loss_attribution_loss_count",
        "advancers",
        "decliners",
        "unchanged",
        "limit_up_count",
        "limit_down_count",
        "cache_age_days",
    }
)
STATUS_COLUMNS.update(
    {
        "is_attention_stock",
        "is_disposition_stock",
        "revenue_12m_high",
        "new_entries_allowed",
        "guardrail_status",
        "loss_attribution_status",
        "loss_bucket",
        "fallback_used",
        "ai_used",
        "margin_price_divergence",
        "pnl_chart_status",
        "market_recap_status",
        "decision_dashboard_status",
        "config_summary_status",
        "enrichment_evidence_status",
        "is_stale_data",
        "top_holdings_available",
        "is_etf",
        "has_industry",
        "has_valuation",
        "has_financials",
        "has_revenue",
        "has_institutional",
        "has_margin",
        "has_event_data",
        "has_etf_metadata",
        "hold",
        "reduce",
        "exit_review",
        "near_stop_loss",
        "near_take_profit",
        "data_quality_warning",
    }
)
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
    strategy_validation = _read_latest_csv(report_dir, "strategy_validation_*.csv")
    trading_decisions = _read_latest_csv(report_dir, "trading_decisions_*.csv")
    loss_attribution = _read_latest_csv(report_dir, "loss_attribution_*.csv")
    market_regime = _read_latest_csv(report_dir, "market_regime_*.csv")
    rejected_orders = _read_latest_csv(report_dir, "rejected_paper_orders_*.csv")
    ai_enrichment = _read_latest_csv(report_dir, "ai_enrichment_*.csv")
    enrichment_evidence = _read_latest_csv(report_dir, "enrichment_evidence_*.csv")
    pnl_chart_data = _read_latest_csv(report_dir, "pnl_chart_data_*.csv")
    market_recap = _read_latest_csv(report_dir, "market_recap_*.csv")
    sector_strength = _read_benchmark_sector_strength(report_dir)
    candidate_coverage = _read_latest_csv(report_dir, "candidate_coverage_report_*.csv")
    position_review = _read_latest_csv(report_dir, "position_review_summary_*.csv")
    missing_industry_priority = _read_csv(report_dir / "missing_industry_priority.csv")
    anysearch_industry_candidates = _read_csv(report_dir / "anysearch_industry_candidates.csv")
    active_config = load_config(ROOT / "config.yaml")
    trading_cost = active_config.get("trading_cost", {})

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
        strategy_validation=strategy_validation,
        trading_decisions=trading_decisions,
        loss_attribution=loss_attribution,
        market_regime=market_regime,
        rejected_orders=rejected_orders,
        ai_enrichment=ai_enrichment,
        enrichment_evidence=enrichment_evidence,
        pnl_chart_data=pnl_chart_data,
        market_recap=market_recap,
        sector_strength=sector_strength,
        candidate_coverage=candidate_coverage,
        position_review=position_review,
        missing_industry_priority=missing_industry_priority,
        anysearch_industry_candidates=anysearch_industry_candidates,
        trading_cost=trading_cost,
        config=active_config,
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
    strategy_validation: pd.DataFrame,
    trading_decisions: pd.DataFrame,
    loss_attribution: pd.DataFrame,
    market_regime: pd.DataFrame,
    rejected_orders: pd.DataFrame,
    ai_enrichment: pd.DataFrame,
    enrichment_evidence: pd.DataFrame,
    pnl_chart_data: pd.DataFrame,
    market_recap: pd.DataFrame,
    sector_strength: pd.DataFrame,
    candidate_coverage: pd.DataFrame,
    position_review: pd.DataFrame,
    missing_industry_priority: pd.DataFrame,
    anysearch_industry_candidates: pd.DataFrame,
    trading_cost: dict[str, object],
    config: dict[str, object] | None = None,
) -> str:
    latest_summary = _first_row(daily_summary)
    data_fetch_status = _read_latest_csv(report_dir, "data_fetch_status_*.csv")
    candidates = _normalize_attention_disposition_display(candidates)
    data_quality_health = _refresh_data_quality_health(report_dir, candidates, data_fetch_status)
    risk_pass = _normalize_attention_disposition_display(_enrich_with_fundamentals(risk_pass, candidates))
    market_intel = _normalize_attention_disposition_display(market_intel)
    enrichment_source = _combined_enrichment_sources(ai_enrichment, candidates, risk_pass, market_intel)
    open_positions = _filter_status(paper_trades, "OPEN")
    closed_trades = _filter_status(paper_trades, "CLOSED")
    latest_paper_summary = _first_row(paper_summary)
    open_positions = _mark_missing_market_context(_enrich_with_fundamentals(open_positions, enrichment_source), enrichment_source)
    open_positions = _enrich_with_local_factor_csv(open_positions)
    open_positions = _apply_holding_risk_lights(open_positions, config or {})
    if position_review.empty:
        try:
            generated_review = generate_position_review_summary(
                report_dir,
                config=config or {},
                trade_date=latest_summary.get("trade_date") if latest_summary else None,
            )
            position_review = generated_review.review
        except Exception:
            position_review = pd.DataFrame()
    pending_orders = _enrich_with_fundamentals(pending_orders, enrichment_source)
    trading_decisions = _enrich_with_fundamentals(trading_decisions, enrichment_source)
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

    market_summary_columns = [
        "stock_id",
        "stock_name",
        "total_score",
        "multi_factor_score",
        "final_market_score",
        "confidence_score",
        "risk_pass",
        "is_attention_stock",
        "is_disposition_stock",
        "review_level",
        "positive_signals",
        "warning_signals",
        "blocking_risks",
        "data_quality_flags",
        "risk_flags",
        "final_comment",
    ]
    market_detail_columns = [
        "rank",
        "trade_date",
        "close",
        "original_total_score",
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
        "attention_reason",
        "disposition_reason",
        "event_reason",
        "institutional_score",
        "institutional_reason",
        "credit_score",
        "event_risk_score",
        "liquidity_score",
        "avg_turnover_20d",
        "slippage_risk_score",
        "sector_strength_score",
        "sector_strength_mode",
        "relative_strength_5d",
        "relative_strength_20d",
        "sector_strength_warning",
        "review_reason",
        "data_source_warning",
        "market_intel_warning",
        "requested_date",
        "actual_data_date",
        "fallback_date",
        "fallback_reason",
        "cache_age_days",
        "is_stale_data",
        "data_freshness_level",
        "system_comment",
        "ai_summary",
        "manual_review_focus",
        "risk_explanation",
        "data_quality_explanation",
        "valuation_context",
        "valuation_risk_level",
        "margin_credit_context",
        "margin_risk_level",
        "margin_price_divergence",
        "industry_main",
        "industry_sub",
        "sector_context",
        "sector_strength_mode",
        "enrichment_provider",
        "ai_used",
        "source_evidence_count",
        "source_evidence_json",
        "market_fundamental_score",
        "market_valuation_score",
        "market_momentum_score",
        "market_chip_score",
        "news_sentiment_score",
        "multi_factor_reason",
        "reason",
    ]
    candidate_detail = _responsive_compact_records(
        candidates,
        market_summary_columns,
        market_detail_columns,
        "目前尚無候選股資料",
        max_rows=20,
    )
    risk_pass_detail = _responsive_compact_records(
        risk_pass,
        market_summary_columns,
        [
            "rank",
            "close",
            "institutional_score",
            "credit_score",
            "event_risk_score",
            "liquidity_score",
            "avg_turnover_20d",
            "slippage_risk_score",
            "sector_strength_score",
            "sector_strength_mode",
            "relative_strength_5d",
            "relative_strength_20d",
            "sector_strength_warning",
            "review_level",
            "review_reason",
            "positive_signals",
            "warning_signals",
            "blocking_risks",
            "momentum_signal",
            "data_quality_flags",
            "investment_risk_flags",
            "attention_reason",
            "disposition_reason",
            "event_reason",
            "event_blocked",
            "event_risk_level",
            "risk_reason",
            "stop_loss_price",
            "suggested_position_pct",
            "data_source_warning",
            "market_intel_warning",
            "system_comment",
            "ai_summary",
            "manual_review_focus",
            "valuation_context",
            "margin_credit_context",
            "sector_context",
            "sector_strength_mode",
            "source_evidence_count",
            "source_evidence_json",
            "multi_factor_reason",
            "reason",
        ],
        "目前尚無通過風控的股票",
        max_rows=20,
    )
    recent_summary_brief = _table(
        _brief_recent_summaries(recent_summaries),
        [
            "summary_requested_date",
            "summary_trade_date",
            "summary_status",
            "summary_candidate_rows",
            "summary_risk_pass_rows",
            "summary_pending_orders",
            "summary_executed_orders",
            "summary_open_positions",
            "summary_closed_positions",
            "summary_total_equity_after_cost",
            "summary_data_status",
        ],
        "目前尚無每日 summary",
        max_rows=10,
    )
    recent_summary_full = _table(
        recent_summaries,
        [column for column in recent_summaries.columns if not column.startswith("_")],
        "目前尚無每日 summary 原始資料",
        max_rows=10,
    )

    overview_content = "".join(
        [
            _pnl_overview(latest_summary, latest_paper_summary, open_positions),
            _overview_dashboard(
                latest_summary,
                latest_paper_summary,
                trading_decisions,
                market_intel,
                missing_industry_priority,
                anysearch_industry_candidates,
            ),
            _benchmark_alpha_section(latest_summary, latest_paper_summary, recent_summaries, market_regime, market_recap, sector_strength),
            _market_regime_score_explainer(latest_summary, market_regime, market_recap),
            _section("今日重點結論", _key_conclusions_v2(latest_summary, data_fetch_status), class_name="key-conclusion-section"),
            _section("今日操作重點", _today_action_summary(latest_summary, pending_orders, open_positions, data_fetch_status, trading_decisions), class_name="today-action-section"),
            _decision_dashboard(latest_summary, trading_decisions, candidates, open_positions),
            _position_review_section(position_review, open_positions),
            _market_recap_section(market_recap, latest_summary),
            _pnl_chart_section(latest_summary, pnl_chart_data, recent_summaries),
            _guardrail_overview(latest_summary, market_regime, rejected_orders),
            _loss_attribution_overview(loss_attribution),
            _enrichment_overview(latest_summary, ai_enrichment),
            _decision_overview(latest_summary, trading_decisions),
            _data_quality_detail_block(latest_summary, data_fetch_status),
            _data_quality_health_section(data_quality_health),
            _details_block("交易成本摘要", _cost_overview(latest_summary, latest_paper_summary, trading_cost)),
            _details_block("紙上交易績效", _paper_performance(latest_paper_summary, closed_trades, open_positions)),
            _details_block("出場策略摘要", _exit_strategy_summary(latest_summary, open_positions, closed_trades)),
            _details_block("非交易日替代交易日說明", _fallback_note(latest_summary)),
        ]
    )
    fundamental_content = "".join(
        [
            _data_confidence_summary(candidates, market_intel, latest_summary, data_fetch_status),
            _enrichment_overview(latest_summary, ai_enrichment),
            _market_intel_summary(candidates, market_intel, latest_summary),
            _multi_factor_summary(candidates, latest_summary),
            _fundamental_summary(candidates),
            _candidate_coverage_section(candidate_coverage),
            _section("候選股", _details_block("今日候選股詳細表", candidate_detail, open_by_default=True), section_id="candidate-detail-section", class_name="candidate-section"),
            _details_block("通過風控股票詳細表", risk_pass_detail),
            _details_block("資料來源依據", _evidence_table(enrichment_evidence)),
        ]
    )
    health_content = "".join(
        [
            _health_summary_cards(health_items),
            _data_quality_health_section(data_quality_health, section_id="data-quality-section"),
            _missing_industry_priority_section(missing_industry_priority),
            _anysearch_industry_candidates_section(anysearch_industry_candidates),
            _data_source_summary_section(data_fetch_status),
            _details_block("資料來源技術細節", _data_source_technical_details(data_fetch_status)),
            _details_block("系統健康檢查詳細項目", _health_section(_non_data_source_health_items(health_items))),
            _details_block("最近每日 summary", recent_summary_brief),
            _details_block("完整每日 summary 原始資料", recent_summary_full),
            _section("系統設定摘要", _details_block("配置說明", _config_summary(config or {}), open_by_default=True), section_id="config-summary-section", class_name="config-summary-section"),
        ]
    )
    decision_content = _decision_engine_content(trading_decisions, strategy_validation)

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
            _section_shortcuts(),
            _tab_panel("overview", "總覽", overview_content, active=True),
            _tab_panel("positions", "目前持倉", _position_cards(open_positions)),
            _tab_panel("pending", "待進場", _pending_cards(pending_orders)),
            _tab_panel("closed", "今日 / 最近已出場", _closed_cards(closed_trades)),
            _tab_panel("fundamental", "市場情報 / 多因子", fundamental_content),
            _tab_panel("decision", "決策引擎", decision_content),
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
        "<small>所有內容僅供紙上模擬交易與策略檢查使用，不代表投資建議，也不承諾投資結果。</small>"
        "</header>"
    )


def _nav_tabs() -> str:
    tabs = [
        ("overview", "總覽"),
        ("positions", "持倉"),
        ("pending", "待進場"),
        ("closed", "已出場"),
        ("fundamental", "市場情報 / 多因子"),
        ("decision", "決策引擎"),
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
    tiles = "".join(
        f'<span class="header-status-tile"><b>{escape(label)}</b><strong>{escape(value)}</strong></span>'
        for label, value in meta
    )
    return (
        '<header class="account-header brokerage-header">'
        '<div class="brokerage-title-block">'
        "<p>TW-Quant Paper Trading</p>"
        "<h1>台股投資儀表板</h1>"
        "<small>紙上交易帳務、風控與資料品質監控；不代表投資建議，也不會真實下單。</small>"
        "</div>"
        f'<div class="header-meta header-status-board">{tiles}</div>'
        "</header>"
    )


def _nav_tabs_v2() -> str:
    tabs = [
        ("overview", "總覽"),
        ("positions", "紙上持倉"),
        ("pending", "待進場"),
        ("closed", "已出場"),
        ("fundamental", "候選股 / 市場情報"),
        ("decision", "今日交易決策"),
        ("health", "資料品質"),
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


def _section_shortcuts() -> str:
    links = [
        ("overview", "dashboard-overview", "總覽"),
        ("decision", "decision-dashboard", "今日交易決策"),
        ("positions", "tab-positions", "紙上持倉"),
        ("fundamental", "candidate-detail-section", "候選股"),
        ("health", "data-quality-section", "資料品質"),
        ("health", "missing-industry-section", "產業分類缺口"),
        ("health", "anysearch-candidates-section", "AnySearch 候選資料"),
        ("health", "config-summary-section", "系統設定摘要"),
    ]
    buttons = "".join(
        f'<button type="button" class="quick-nav-link" data-tab-jump="{escape(tab)}" '
        f'data-section-target="{escape(target)}">{escape(label)}</button>'
        for tab, target, label in links
    )
    return f'<nav class="quick-section-nav" aria-label="重點區塊導覽">{buttons}</nav>'


def _tab_panel(panel_id: str, title: str, content: str, active: bool = False) -> str:
    classes = "tab-panel active" if active else "tab-panel"
    title = {
        "overview": "總覽",
        "positions": "目前持倉",
        "pending": "待進場",
        "closed": "今日 / 最近已出場",
        "fundamental": "市場情報 / 多因子",
        "decision": "今日交易決策 / 決策引擎",
        "health": "系統健康檢查",
    }.get(panel_id, title)
    return (
        f'<section id="tab-{escape(panel_id)}" class="{classes}" data-tab-panel="{escape(panel_id)}" '
        f'role="tabpanel"><h2>{escape(title)}</h2>{content}</section>'
    )


def _overview_dashboard(
    summary: dict[str, object],
    paper_summary: dict[str, object],
    decisions: pd.DataFrame,
    market_intel: pd.DataFrame,
    missing_industry_priority: pd.DataFrame,
    anysearch_candidates: pd.DataFrame,
) -> str:
    data_frame = market_intel
    requested = _format_cell("requested_date", summary.get("requested_date") or summary.get("trade_date"))
    trade_date = _format_cell("trade_date", summary.get("trade_date"))
    actual_data_date = _market_intel_actual_data_date(summary, data_frame)
    freshness_level = _market_intel_freshness_level(summary, data_frame)
    market_status = _first_raw(summary.get("market_intel_status"), _frame_first(data_frame, "market_intel_status"), summary.get("status"))
    guardrail_status = _first_raw(summary.get("guardrail_status"), "UNKNOWN")
    using_cache = str(summary.get("market_intel_status", "")).strip().upper() == "CACHE"
    if not data_frame.empty and "market_intel_status" in data_frame.columns:
        using_cache = using_cache or data_frame["market_intel_status"].fillna("").astype(str).str.upper().eq("CACHE").any()
    stale = _market_intel_is_stale(summary, data_frame) or freshness_level in {"STALE", "CACHE"} or using_cache
    freshness_note = (
        "市場資料過期，不建議短線進場"
        if stale
        else "資料為最新或目前可用資料"
    )

    equity_source = paper_summary or summary
    total_equity = _format_cell("total_equity", _first_raw(
        equity_source.get("total_equity_after_cost"),
        equity_source.get("total_equity"),
        summary.get("total_equity_after_cost"),
        summary.get("total_equity"),
    ))
    unrealized = _format_cell("unrealized_pnl", _first_raw(equity_source.get("unrealized_pnl"), summary.get("unrealized_pnl")))
    realized = _format_cell("realized_pnl_after_cost", _first_raw(
        equity_source.get("realized_pnl_after_cost"),
        equity_source.get("realized_pnl"),
        summary.get("realized_pnl_after_cost"),
        summary.get("realized_pnl"),
    ))

    buy_count = _summary_or_decision_count(summary, decisions, "buy_candidate_count", "BUY_CANDIDATE")
    watch_count = _summary_or_decision_count(summary, decisions, "watch_only_count", "WATCH_ONLY")
    no_trade_count = _summary_or_decision_count(summary, decisions, "no_trade_count", "NO_TRADE")
    hold_count = _summary_or_decision_count(summary, decisions, "hold_count", "HOLD")
    reduce_count = _summary_or_decision_count(summary, decisions, "reduce_count", "REDUCE")
    exit_count = _summary_or_decision_count(summary, decisions, "exit_review_count", "EXIT")

    high_priority = _priority_level_count(missing_industry_priority, "HIGH")
    medium_priority = _priority_level_count(missing_industry_priority, "MEDIUM")
    urgent_priority = high_priority + medium_priority
    pending_review = _anysearch_status_count(anysearch_candidates, "PENDING_REVIEW")
    manual_check = _anysearch_status_count(anysearch_candidates, "NEEDS_MANUAL_CHECK")

    cards = [
        _kpi_card(
            "今日市場資料狀態",
            _status_badge(market_status, "market_intel_status") + _status_badge(freshness_level, "data_freshness_level", "freshness-badge"),
            [
                ("報表日期", requested),
                ("實際交易日", trade_date),
                ("實際資料日", actual_data_date),
                ("快取 / 資料年齡", _market_intel_cache_age_text(data_frame)),
            ],
            tone="danger" if stale else "ok",
        ),
        _kpi_card(
            "交易安全狀態",
            _status_badge(guardrail_status, "guardrail_status", "guardrail-badge"),
            [
                ("是否允許新增持倉", _format_cell("new_entries_allowed", summary.get("new_entries_allowed"))),
                ("暫停新倉原因", _format_cell("pause_new_entries_reason", summary.get("pause_new_entries_reason"))),
            ],
            tone="danger" if str(summary.get("guardrail_status", "")).upper() == "BLOCKED" else "ok",
        ),
        _kpi_card(
            "紙上交易資產",
            escape(total_equity),
            [
                ("未實現損益", unrealized),
                ("已實現損益", realized),
            ],
            tone="neutral",
        ),
        _kpi_card(
            "今日決策統計",
            escape(f"BUY {buy_count:,.0f} / WATCH {watch_count:,.0f} / NO_TRADE {no_trade_count:,.0f}"),
            [
                ("HOLD", f"{hold_count:,.0f}"),
                ("REDUCE", f"{reduce_count:,.0f}"),
                ("EXIT_REVIEW", f"{exit_count:,.0f}"),
            ],
            tone="warning" if buy_count and stale else "neutral",
        ),
        _kpi_card(
            "產業分類缺口",
            escape(
                "高優先缺口已清空"
                if urgent_priority == 0
                else f"HIGH {high_priority:,.0f} / MEDIUM {medium_priority:,.0f}"
            ),
            [
                ("需優先處理", f"{urgent_priority:,.0f}"),
                ("HIGH", f"{high_priority:,.0f}"),
                ("MEDIUM", f"{medium_priority:,.0f}"),
            ],
            tone="ok" if urgent_priority == 0 else "warning",
        ),
        _kpi_card(
            "AnySearch 候選資料",
            escape(f"{len(anysearch_candidates):,.0f} 筆"),
            [
                ("PENDING_REVIEW", f"{pending_review:,.0f}"),
                ("NEEDS_MANUAL_CHECK", f"{manual_check:,.0f}"),
                ("狀態", "候選資料，尚未正式採用"),
            ],
            tone="info" if len(anysearch_candidates) else "neutral",
        ),
    ]
    alert_class = "dashboard-alert danger" if stale else "dashboard-alert ok"
    return _section(
        "總覽儀表板",
        f'<div class="{alert_class}">{escape(freshness_note)}</div>'
        + '<div class="kpi-grid">'
        + "".join(cards)
        + "</div>",
        section_id="dashboard-overview",
        class_name="dashboard-overview-section",
    )


def _summary_or_decision_count(summary: dict[str, object], decisions: pd.DataFrame, summary_key: str, decision_value: str) -> int:
    summary_value = _to_float(summary.get(summary_key))
    if summary_value is not None:
        return int(summary_value)
    return _decision_count(decisions, "decision", decision_value)


def _kpi_card(title: str, primary_html: str, metrics: list[tuple[str, str]], tone: str = "neutral") -> str:
    metric_items = "".join(
        f'<span><b>{escape(label)}</b><em>{escape(value)}</em></span>'
        for label, value in metrics
    )
    return (
        f'<article class="kpi-card {escape(tone)}">'
        f"<h3>{escape(title)}</h3>"
        f'<div class="kpi-primary">{primary_html}</div>'
        f'<div class="kpi-meta">{metric_items}</div>'
        "</article>"
    )


def _status_badge(value: object, column: str = "status", extra_class: str = "") -> str:
    raw = "-" if _is_blank(value) else str(value).strip()
    normalized = raw.upper().replace(" ", "_")
    label = _format_cell(column, raw)
    css_class = _badge_class(normalized)
    if extra_class:
        css_class += f" {extra_class}"
    return f'<span class="status-badge {escape(css_class)}">{escape(label)}</span>'


def _badge_class(normalized: str) -> str:
    if normalized in {"OK", "CURRENT", "OPEN", "HOLD"}:
        return "badge-ok"
    if normalized in {"RECENT", "PENDING_REVIEW", "WATCH_ONLY", "INFO", "WATCH"}:
        return "badge-info"
    if normalized in {"OK_WITH_FALLBACK", "OK_WITH_WARNING", "CACHE", "NEEDS_MANUAL_CHECK", "REDUCE", "MEDIUM", "ATTENTION", "WARNING"}:
        return "badge-warning"
    if normalized in {"STALE", "BLOCKED", "FAILED", "MISSING", "NO_TRADE", "EXIT", "HIGH", "HIGH_RISK"}:
        return "badge-danger"
    return "badge-neutral"


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
    if not open_positions.empty and "risk_light" in open_positions.columns:
        lights = open_positions["risk_light"].fillna("").astype(str)
        secondary.extend(
            [
                ("持倉紅燈數", f"{(lights == '紅燈').sum():,.0f}", None),
                ("持倉黃燈數", f"{(lights == '黃燈').sum():,.0f}", None),
                ("持倉綠燈數", f"{(lights == '綠燈').sum():,.0f}", None),
            ]
        )
    primary_cards = "".join(_overview_metric(label, value, raw, class_name) for label, value, raw, class_name in primary)
    secondary_cards = "".join(_overview_metric(label, value, raw, "") for label, value, raw in secondary)
    donut_card = _asset_pnl_donut_card(
        total_equity=total_equity_after_cost,
        total_capital=total_capital,
        invested_value=invested_value,
        market_value=market_value,
        cash=_first_number(summary, "cash"),
        unrealized=unrealized,
        realized=realized,
        total_pnl=total_pnl,
        return_pct=return_pct,
        new_entries_allowed=_format_cell(
            "new_entries_allowed",
            _first_raw(daily_summary.get("new_entries_allowed"), summary.get("new_entries_allowed")),
        ),
        open_positions=open_positions,
    )
    kpi_panel = f'<div class="pnl-card pnl-kpi-panel"><h3>損益 KPI 總覽</h3><div class="pnl-primary">{primary_cards}</div><div class="pnl-secondary">{secondary_cards}</div></div>'
    return _section(
        "損益總覽",
        f'<div class="pnl-overview-layout">{donut_card}{kpi_panel}</div>',
        section_id="pnl-overview",
        class_name="pnl-overview-section",
    )


def _asset_pnl_donut_card(
    *,
    total_equity: float | None,
    total_capital: float | None,
    invested_value: float | None,
    market_value: float | None,
    cash: float | None,
    unrealized: float | None,
    realized: float | None,
    total_pnl: float | None,
    return_pct: float | None,
    new_entries_allowed: str,
    open_positions: pd.DataFrame,
) -> str:
    display_market_value = market_value if market_value is not None else invested_value
    has_allocation = bool(total_equity and total_equity > 0 and display_market_value is not None)
    if not has_allocation:
        fallback_metrics = "".join(
            _overview_metric(label, value, raw, "")
            for label, value, raw in [
                ("總資產", _format_number_or_dash(total_equity), None),
                ("總報酬率", _percent_or_dash(return_pct), total_pnl),
                ("未實現損益", _signed_or_dash(unrealized), unrealized),
                ("已實現損益", _signed_or_dash(realized), realized),
                ("是否允許新增持倉", new_entries_allowed, None),
            ]
        )
        return (
            '<div class="asset-donut-card asset-donut-fallback">'
            "<h3>資產 / 損益圓環卡</h3>"
            '<p class="note">持倉資料不足，改顯示損益摘要。</p>'
            f'<div class="pnl-secondary">{fallback_metrics}</div>'
            "</div>"
        )

    holding_pct = max(0.0, min(100.0, (display_market_value or 0.0) / total_equity * 100.0))
    cash_value = cash if cash is not None else max(0.0, total_equity - (display_market_value or 0.0))
    cash_pct = max(0.0, min(100.0, cash_value / total_equity * 100.0)) if total_equity else 0.0
    allocation_items = _asset_allocation_items(open_positions, total_equity)
    bottom_metrics = [
        ("總成本", _format_number_or_dash(invested_value), None),
        ("未實現損益", _signed_or_dash(unrealized), unrealized),
        ("已實現損益", _signed_or_dash(realized), realized),
        ("是否允許新增持倉", new_entries_allowed, None),
    ]
    bottom_html = "".join(_asset_bottom_metric(label, value, raw) for label, value, raw in bottom_metrics)
    return (
        '<div class="asset-donut-card">'
        '<div class="asset-donut-head"><h3>資產 / 損益圓環卡</h3><span>持倉現值比例 / 總資產</span></div>'
        '<div class="asset-donut-body">'
        '<div class="asset-visual-stack">'
        f'<div class="asset-donut" style="--holding-pct:{holding_pct:.2f}%;" role="img" aria-label="持倉現值占總資產 {holding_pct:.1f}%">'
        '<div class="asset-donut-core">'
        '<span>總資產</span>'
        f'<strong>{escape(_format_number_or_dash(total_equity))}</strong>'
        f'<em class="{_profit_class(total_pnl)}">總報酬率 {_percent_or_dash(return_pct)}</em>'
        "</div></div>"
        f'<div class="asset-pnl-bottom">{bottom_html}</div>'
        "</div>"
        '<div class="asset-allocation">'
        '<div class="allocation-row"><span><i class="asset-dot holding-dot"></i>持倉現值</span>'
        f'<strong>{holding_pct:.1f}%</strong></div>'
        '<div class="allocation-row"><span><i class="asset-dot cash-dot"></i>現金 / 未投入</span>'
        f'<strong>{cash_pct:.1f}%</strong></div>'
        '<h4>前幾大持倉</h4>'
        f"{allocation_items}"
        "</div></div>"
        "</div>"
    )


def _asset_allocation_items(open_positions: pd.DataFrame, total_equity: float) -> str:
    if open_positions.empty or total_equity <= 0:
        return '<div class="empty compact-empty">目前尚無持倉占比資料</div>'
    value_column = "market_value" if "market_value" in open_positions.columns else "position_value"
    if value_column not in open_positions.columns:
        return '<div class="empty compact-empty">目前尚無持倉占比資料</div>'
    rows = open_positions.copy()
    rows["_allocation_value"] = pd.to_numeric(rows[value_column], errors="coerce").fillna(0.0)
    rows = rows[rows["_allocation_value"] > 0].sort_values("_allocation_value", ascending=False).head(4)
    if rows.empty:
        return '<div class="empty compact-empty">目前尚無持倉占比資料</div>'
    items = []
    for _, row in rows.iterrows():
        stock_name = _format_cell("stock_name", row.get("stock_name"))
        stock_id = _format_cell("stock_id", row.get("stock_id"))
        pct = float(row["_allocation_value"]) / total_equity * 100.0
        items.append(
            "<li>"
            f"<span><b>{escape(stock_name)}</b><em>{escape(stock_id)}</em></span>"
            f"<strong>{pct:.1f}%</strong>"
            "</li>"
        )
    return f'<ol class="allocation-list">{"".join(items)}</ol>'


def _asset_bottom_metric(label: str, value: str, raw_value: float | None) -> str:
    value_class = _profit_class(raw_value) if raw_value is not None else "profit-flat"
    return (
        '<div class="asset-bottom-metric">'
        f"<span>{escape(label)}</span>"
        f'<strong class="{value_class}">{escape(value)}</strong>'
        "</div>"
    )


def _pnl_chart_section(
    summary: dict[str, object],
    pnl_chart_data: pd.DataFrame,
    recent_summaries: pd.DataFrame,
) -> str:
    source = pnl_chart_data if not pnl_chart_data.empty else recent_summaries
    if source.empty:
        return _section("今日損益圖", _empty("損益資料不足"))
    row = source.iloc[-1].to_dict()
    unrealized = _first_number(row, "unrealized_pnl")
    realized_today = _first_number(row, "realized_pnl_after_cost")
    if realized_today is None:
        realized_today = _first_number(summary, "realized_pnl_after_cost_today")
    today_total = _first_number(row, "today_total_pnl")
    if today_total is None:
        today_total = (unrealized or 0.0) + (realized_today or 0.0)
    cumulative = _first_number(summary, "realized_pnl_after_cost")
    total_equity = _first_number(row, "total_equity_after_cost") or _first_number(summary, "total_equity_after_cost")
    previous_equity = None
    if len(source) >= 2:
        previous_equity = _first_number(source.iloc[-2].to_dict(), "total_equity_after_cost")
    equity_change = (total_equity - previous_equity) if total_equity is not None and previous_equity is not None else None
    bars = [
        ("今日未實現損益", unrealized),
        ("今日已實現損益", realized_today),
        ("今日總損益", today_total),
        ("累計已實現損益", cumulative),
        ("總資產變化", equity_change),
    ]
    max_abs = max([abs(value or 0.0) for _, value in bars] + [1.0])
    bar_html = "".join(_pnl_bar(label, value, max_abs) for label, value in bars)
    line_chart = _pnl_line_chart(source)
    return _section(
        "今日損益圖",
        f'<div class="chart-grid"><div class="chart-card"><h3>今日損益摘要</h3>{bar_html}</div>'
        f'<div class="chart-card"><h3>近期資產 / 損益趨勢</h3>{line_chart}</div></div>',
    )


def _pnl_bar(label: str, value: float | None, max_abs: float) -> str:
    raw = value or 0.0
    width = min(100.0, abs(raw) / max_abs * 100.0)
    return (
        '<div class="pnl-bar-row">'
        f'<span>{escape(label)}</span>'
        f'<div class="pnl-bar-track"><i class="{_profit_class(raw)}" style="width:{width:.1f}%"></i></div>'
        f'<strong class="{_profit_class(raw)}">{escape(_signed_or_dash(raw))}</strong>'
        "</div>"
    )


def _pnl_line_chart(frame: pd.DataFrame) -> str:
    if frame.empty:
        return _empty("損益資料不足")
    rows = frame.tail(20).copy()
    series_columns = [
        ("total_equity_after_cost", "扣成本後總資產", "#f5f5f5"),
        ("unrealized_pnl", "未實現損益", "#ef4444"),
        ("realized_pnl_after_cost", "已實現損益", "#22c55e"),
    ]
    all_values: list[float] = []
    for column, _, _ in series_columns:
        if column in rows.columns:
            all_values.extend(pd.to_numeric(rows[column], errors="coerce").dropna().astype(float).tolist())
    if not all_values:
        return _empty("損益資料不足")
    min_value = min(all_values)
    max_value = max(all_values)
    if min_value == max_value:
        min_value -= 1
        max_value += 1
    width, height, pad = 360, 160, 18
    polylines = []
    for column, label, color in series_columns:
        if column not in rows.columns:
            continue
        values = pd.to_numeric(rows[column], errors="coerce").tolist()
        points = []
        for index, value in enumerate(values):
            if pd.isna(value):
                continue
            x = pad + (width - pad * 2) * (index / max(len(values) - 1, 1))
            y = height - pad - (height - pad * 2) * ((float(value) - min_value) / (max_value - min_value))
            points.append(f"{x:.1f},{y:.1f}")
        if points:
            polylines.append(f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" stroke-width="2" />')
    legend = "".join(
        f'<span><i style="background:{color}"></i>{escape(label)}</span>' for _, label, color in series_columns
    )
    return (
        f'<svg class="pnl-line-chart" viewBox="0 0 {width} {height}" role="img" aria-label="近期資產與損益趨勢">'
        f'<line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{height-pad}" stroke="#334155" />'
        + "".join(polylines)
        + "</svg>"
        + f'<div class="chart-legend">{legend}</div>'
    )


def _decision_dashboard(
    summary: dict[str, object],
    decisions: pd.DataFrame,
    candidates: pd.DataFrame,
    open_positions: pd.DataFrame,
) -> str:
    decision_stats = [
        ("BUY_CANDIDATE", "買進候選", _decision_count(decisions, "decision", "BUY_CANDIDATE"), "buy"),
        ("WATCH_ONLY", "觀察名單", _decision_count(decisions, "decision", "WATCH_ONLY"), "watch"),
        ("NO_TRADE", "不交易", _decision_count(decisions, "decision", "NO_TRADE"), "no-trade"),
        ("HOLD", "持倉檢查", _decision_count(decisions, "decision", "HOLD"), "hold"),
        ("REDUCE", "降低風險檢查", _decision_count(decisions, "decision", "REDUCE"), "reduce"),
        ("EXIT_REVIEW", "出場檢查", _decision_count(decisions, "decision", "EXIT"), "exit"),
    ]
    stats = [
        (label, value)
        for label, _, value, _ in decision_stats
    ] + [
        ("A 級候選", _decision_count(decisions, "candidate_grade", "A") or int(_to_float(summary.get("grade_a_count")) or 0)),
        ("B 級候選", _decision_count(decisions, "candidate_grade", "B") or int(_to_float(summary.get("grade_b_count")) or 0)),
        ("C 級候選", _decision_count(decisions, "candidate_grade", "C") or int(_to_float(summary.get("grade_c_count")) or 0)),
        ("D 級候選", _decision_count(decisions, "candidate_grade", "D") or int(_to_float(summary.get("grade_d_count")) or 0)),
    ]
    lanes = "".join(
        '<div class="decision-lane decision-{}">'
        '<span>{}</span><strong>{:,.0f}</strong><em>{}</em></div>'.format(
            escape(tone),
            escape(label),
            value,
            escape(caption),
        )
        for label, caption, value, tone in decision_stats
    )
    stat_cards = '<div class="cards decision-stat-cards">' + "".join(_card(label, f"{value:,.0f}") for label, value in stats) + "</div>"
    top = decisions.copy()
    if not top.empty:
        score = top["final_market_score"] if "final_market_score" in top.columns else top.get("multi_factor_score", pd.Series([0] * len(top)))
        top["_dashboard_score"] = pd.to_numeric(score, errors="coerce").fillna(0)
        top = top.sort_values("_dashboard_score", ascending=False).head(5)
    top_table = _responsive_compact_records(
        top,
        ["stock_id", "stock_name", "decision", "candidate_grade", "total_score", "multi_factor_score", "final_market_score", "confidence_score"],
        ["reason", "risk_flags", "final_comment", "ai_summary", "manual_review_focus"],
        "目前尚無決策重點股票",
        5,
    )
    risks = _risk_alert_list(summary, candidates, decisions, open_positions)
    catalysts = _catalyst_list(candidates, decisions)
    content = (
        '<div class="decision-lanes">' + lanes + "</div>"
        + stat_cards
        + _details_block("分析結果摘要", top_table, open_by_default=True)
        + '<div class="dashboard-split">'
        + _section("風險警報", '<ul class="risk-list">' + "".join(f"<li>{escape(item)}</li>" for item in risks) + "</ul>")
        + _section("利好催化", '<ul class="catalyst-list">' + "".join(f"<li>{escape(item)}</li>" for item in catalysts) + "</ul>")
        + "</div>"
        + '<p class="note">買進候選需人工確認；出場訊號檢查需人工確認；本系統未自動下單。</p>'
    )
    return _section("決策儀表盤", content, section_id="decision-dashboard", class_name="decision-dashboard-section")


def _risk_alert_list(
    summary: dict[str, object],
    candidates: pd.DataFrame,
    decisions: pd.DataFrame,
    open_positions: pd.DataFrame,
) -> list[str]:
    items: list[str] = []
    regime_score = _to_float(summary.get("market_regime_score"))
    if regime_score is not None and 0 < regime_score < 60:
        items.append(f"market_regime_score {regime_score:.0f} 偏低，紙上新增持倉需保守。")
    if str(summary.get("guardrail_status", "")).upper() == "BLOCKED":
        items.append("Guardrail 狀態為 BLOCKED，新增或執行 pending order 需暫停。")
    cancelled = int(_to_float(summary.get("pending_orders_cancelled_count")) or 0)
    if cancelled:
        items.append(f"有 {cancelled} 筆 pending order 被取消，需檢查 rejected report。")
    for frame in [candidates, decisions, open_positions]:
        if frame.empty:
            continue
        for _, row in frame.head(20).iterrows():
            flags = _safe_text(row.get("risk_flags"))
            if any(keyword in flags for keyword in ["處置股", "注意股", "PE 偏高", "融資", "流動性", "產業資料不足"]):
                items.append(f"{_format_cell('stock_id', row.get('stock_id'))} {_format_cell('stock_name', row.get('stock_name'))}：{flags}")
            if _to_float(row.get("liquidity_score")) is not None and (_to_float(row.get("liquidity_score")) or 0) < 50:
                items.append(f"{_format_cell('stock_id', row.get('stock_id'))} 流動性分數偏低，短線滑價風險較高。")
            if str(row.get("event_risk_level", "")).upper() == "HIGH":
                items.append(f"{_format_cell('stock_id', row.get('stock_id'))} 高風險事件，需人工確認。")
            if len(items) >= 5:
                return items[:5]
    return items[:5] or ["目前未彙整到重大風險警報，仍需人工檢查資料品質。"]


def _catalyst_list(candidates: pd.DataFrame, decisions: pd.DataFrame) -> list[str]:
    items: list[str] = []
    combined = pd.concat([frame for frame in [candidates, decisions] if not frame.empty], ignore_index=True) if (not candidates.empty or not decisions.empty) else pd.DataFrame()
    if combined.empty:
        return ["目前尚無利好催化資料。"]
    for _, row in combined.head(30).iterrows():
        stock = f"{_format_cell('stock_id', row.get('stock_id'))} {_format_cell('stock_name', row.get('stock_name'))}".strip()
        if (_to_float(row.get("revenue_yoy")) or 0) > 0:
            items.append(f"{stock} 月營收年增為正，可列入觀察。")
        if (_to_float(row.get("institutional_score")) or 0) >= 65 or "法人買超" in _safe_text(row.get("risk_flags")):
            items.append(f"{stock} 籌碼面偏多，需搭配價格與量能確認。")
        if (_to_float(row.get("sector_strength_score")) or 0) >= 65:
            items.append(f"{stock} 相對強弱分數偏高，但仍需確認比較基準。")
        if (_to_float(row.get("liquidity_score")) or 0) >= 70:
            items.append(f"{stock} 流動性較佳，紙上成交假設相對可檢查。")
        if len(items) >= 5:
            return items[:5]
    return items[:5] or ["目前尚無明確利好催化，請以候選股理由與風控檢查為主。"]


def _position_review_section(position_review: pd.DataFrame, open_positions: pd.DataFrame) -> str:
    total = len(position_review) if not position_review.empty else len(open_positions)
    if position_review.empty:
        content = '<div class="cards">' + _card("目前持倉", f"{total:,.0f}") + "</div>" + _empty("目前尚無 position_review_summary")
        return _section("持倉狀態整理", content)

    cards = [
        ("目前持倉", f"{total:,.0f}"),
        ("續抱", f"{_count_true(position_review, 'hold'):,.0f}"),
        ("降低風險檢查", f"{_count_true(position_review, 'reduce'):,.0f}"),
        ("出場檢查", f"{_count_true(position_review, 'exit_review'):,.0f}"),
        ("接近停損", f"{_count_true(position_review, 'near_stop_loss'):,.0f}"),
        ("接近停利", f"{_count_true(position_review, 'near_take_profit'):,.0f}"),
        ("資料品質警示", f"{_count_true(position_review, 'data_quality_warning'):,.0f}"),
    ]
    table = _table(
        position_review,
        [
            "trade_date",
            "stock_id",
            "stock_name",
            "decision",
            "hold",
            "reduce",
            "exit_review",
            "near_stop_loss",
            "near_take_profit",
            "data_quality_warning",
            "current_price",
            "stop_loss_price",
            "unrealized_pnl_pct",
            "risk_light",
            "review_reason",
        ],
        "目前尚無持倉狀態整理",
        50,
    )
    note = f'<p class="note">目前 {total:,.0f} 檔持倉中各類數量如下；僅供人工檢查，不會自動賣出。</p>'
    return _section(
        "持倉狀態整理",
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + note
        + _details_block("position_review_summary 明細", table),
    )


def _candidate_coverage_section(candidate_coverage: pd.DataFrame) -> str:
    if candidate_coverage.empty:
        return _section("候選股資料覆蓋率", _empty("目前尚無 candidate_coverage_report"))

    total = len(candidate_coverage)
    missing = candidate_coverage["missing_fields"].fillna("").astype(str).str.strip() if "missing_fields" in candidate_coverage.columns else pd.Series([""] * total)
    etf_missing = int(missing.str.contains("ETF_METADATA_MISSING", na=False).sum())
    financial_missing = int(missing.str.contains("FINANCIAL_MISSING", na=False).sum())
    cards = [
        ("候選股檢查數", f"{total:,.0f}"),
        ("資料完整候選", f"{int((missing == '').sum()):,.0f}"),
        ("缺產業分類", f"{_coverage_missing_count(candidate_coverage, 'INDUSTRY_MISSING'):,.0f}"),
        ("缺估值資料", f"{_coverage_missing_count(candidate_coverage, 'VALUATION_MISSING'):,.0f}"),
        ("缺財報資料", f"{financial_missing:,.0f}"),
        ("ETF metadata 缺失", f"{etf_missing:,.0f}"),
    ]
    table = _table(
        candidate_coverage,
        [
            "trade_date",
            "stock_id",
            "stock_name",
            "is_etf",
            "decision",
            "candidate_grade",
            "has_industry",
            "has_valuation",
            "has_financials",
            "has_revenue",
            "has_institutional",
            "has_margin",
            "has_event_data",
            "has_etf_metadata",
            "missing_fields",
        ],
        "目前尚無候選股資料覆蓋率明細",
        50,
    )
    note = '<p class="note">ETF 不以 EPS / ROE 缺失列為一般個股財報缺失；ETF metadata 未接來源時標示 ETF_METADATA_MISSING。</p>'
    return _section(
        "候選股資料覆蓋率",
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + note
        + _details_block("candidate_coverage_report 明細", table),
    )


def _coverage_missing_count(frame: pd.DataFrame, code: str) -> int:
    if frame.empty or "missing_fields" not in frame.columns:
        return 0
    return int(frame["missing_fields"].fillna("").astype(str).str.contains(code, na=False).sum())


def _missing_industry_priority_section(priority: pd.DataFrame) -> str:
    title = "缺產業分類優先補資料清單"
    if priority.empty:
        cards = [
            ("缺分類總數", "0"),
            ("HIGH", "0"),
            ("MEDIUM", "0"),
            ("需優先處理", "0"),
        ]
        return _section(
            title,
            '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
            + '<p class="note">高優先缺口已清空。</p>'
            + _empty("目前尚無 missing_industry_priority.csv"),
            section_id="missing-industry-section",
            class_name="missing-industry-section",
        )

    frame = priority.copy()
    frame["_priority_score"] = pd.to_numeric(frame.get("priority_score"), errors="coerce").fillna(0)
    frame["_recent_appearance_count"] = pd.to_numeric(frame.get("recent_appearance_count"), errors="coerce").fillna(0)
    frame = frame.sort_values(
        ["_priority_score", "_recent_appearance_count", "stock_id"],
        ascending=[False, False, True],
    )
    high = frame[frame.get("priority_level", pd.Series([""] * len(frame))).fillna("").astype(str).str.upper() == "HIGH"]
    medium = frame[frame.get("priority_level", pd.Series([""] * len(frame))).fillna("").astype(str).str.upper() == "MEDIUM"]
    low = frame[frame.get("priority_level", pd.Series([""] * len(frame))).fillna("").astype(str).str.upper() == "LOW"]
    urgent = pd.concat([high, medium], ignore_index=False)
    top = high if not high.empty else medium
    top = top.drop(columns=["_priority_score", "_recent_appearance_count"], errors="ignore")
    urgent_count = len(urgent)
    cards = [
        ("缺分類總數", f"{len(priority):,.0f}"),
        ("HIGH", f"{_priority_level_count(priority, 'HIGH'):,.0f}"),
        ("MEDIUM", f"{_priority_level_count(priority, 'MEDIUM'):,.0f}"),
        ("需優先處理", f"{urgent_count:,.0f}"),
    ]
    table = _table(
        top,
        [
            "stock_id",
            "stock_name",
            "market_type",
            "latest_relative_mode",
            "fallback_reason",
            "appear_in_candidates_count",
            "appear_in_trading_decisions_count",
            "appear_in_risk_pass_count",
            "appear_in_position_review_count",
            "appear_in_ai_enrichment_count",
            "recent_appearance_count",
            "liquidity_score",
            "avg_volume",
            "turnover_value",
            "last_seen_date",
            "priority_score",
            "priority_level",
            "suggested_action",
        ],
        "目前尚無缺產業分類優先補資料明細",
        20,
    )
    medium_table = _table(medium.drop(columns=["_priority_score", "_recent_appearance_count"], errors="ignore"), [
        "stock_id",
        "stock_name",
        "market_type",
        "latest_relative_mode",
        "priority_score",
        "priority_level",
        "suggested_action",
    ], "目前尚無 MEDIUM priority 缺口", 20)
    low_table = _table(low.drop(columns=["_priority_score", "_recent_appearance_count"], errors="ignore"), [
        "stock_id",
        "stock_name",
        "market_type",
        "latest_relative_mode",
        "priority_score",
        "priority_level",
        "suggested_action",
    ], "目前尚無 LOW priority 缺口", 20)
    if high.empty and medium.empty:
        high_note = "高優先缺口已清空；MEDIUM 缺口也已清空。LOW priority 已移至下方收合區作為背景資訊。"
    elif high.empty:
        high_note = "高優先缺口已清空；MEDIUM priority 缺口仍需排入下一批補資料。"
    else:
        high_note = "HIGH priority 缺口已置頂，優先人工查證。"
    note = (
        f'<p class="note">{escape(high_note)} 此清單只列出仍使用 market_relative_fallback 的股票；'
        "不會移除缺產業分類警告，也不會自動補分類。</p>"
    )
    return _section(
        title,
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + note
        + _details_block("HIGH / MEDIUM 優先補資料標的", table, open_by_default=urgent_count > 0)
        + _details_block("MEDIUM priority 缺口", medium_table)
        + _details_block(
            f"LOW priority 低優先缺口（{len(low):,.0f} 筆，收合資訊）",
            low_table,
            class_name="missing-low-priority-details",
        ),
        section_id="missing-industry-section",
        class_name="missing-industry-section",
    )


def _priority_level_count(frame: pd.DataFrame, level: str) -> int:
    if frame.empty or "priority_level" not in frame.columns:
        return 0
    return int((frame["priority_level"].fillna("").astype(str).str.upper() == level).sum())


def _anysearch_industry_candidates_section(candidates: pd.DataFrame) -> str:
    title = "AnySearch 產業分類候選資料"
    cards = [
        ("查詢筆數", f"{len(candidates):,.0f}"),
        ("PENDING_REVIEW", f"{_anysearch_status_count(candidates, 'PENDING_REVIEW'):,.0f}"),
        ("NEEDS_MANUAL_CHECK", f"{_anysearch_status_count(candidates, 'NEEDS_MANUAL_CHECK'):,.0f}"),
    ]
    status_badges = (
        '<div class="status-strip">'
        + _status_badge("PENDING_REVIEW", "status")
        + _status_badge("NEEDS_MANUAL_CHECK", "status")
        + "</div>"
    )
    note = '<p class="note">此為候選資料，需人工確認後才可寫入正式產業分類。候選資料，尚未正式採用。</p>'
    if candidates.empty:
        return _section(
            title,
            '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
            + status_badges
            + note
            + _empty("目前尚無 anysearch_industry_candidates.csv 候選資料"),
            section_id="anysearch-candidates-section",
            class_name="anysearch-candidates-section",
        )

    frame = candidates.copy()
    frame["_confidence"] = pd.to_numeric(frame.get("confidence"), errors="coerce").fillna(0)
    frame = frame.sort_values(["_confidence", "stock_id"], ascending=[False, True]).drop(columns=["_confidence"], errors="ignore")
    table = _table(
        frame,
        [
            "stock_id",
            "stock_name",
            "proposed_market_type",
            "proposed_industry",
            "proposed_sub_industry",
            "source_title",
            "source_url",
            "source_type",
            "confidence",
            "reason",
            "status",
            "checked_at",
        ],
        "目前尚無 AnySearch 產業分類候選資料",
        10,
    )
    return _section(
        title,
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + status_badges
        + note
        + _details_block("前 10 筆候選資料", table, open_by_default=True),
        section_id="anysearch-candidates-section",
        class_name="anysearch-candidates-section",
    )


def _anysearch_status_count(frame: pd.DataFrame, status: str) -> int:
    if frame.empty or "status" not in frame.columns:
        return 0
    return int((frame["status"].fillna("").astype(str).str.upper() == status).sum())


def _market_recap_section(market_recap: pd.DataFrame, summary: dict[str, object]) -> str:
    if market_recap.empty:
        fallback = pd.DataFrame(
            [
                {
                    "trade_date": summary.get("trade_date", ""),
                    "market_regime_score": summary.get("market_regime_score", ""),
                    "regime_label": "大盤復盤資料不足",
                    "recap_summary": "尚無 market_recap 資料，使用 daily summary 市場環境分數 fallback。",
                    "fallback_used": True,
                    "data_quality_note": "market_recap CSV 尚未產生",
                }
            ]
        )
        market_recap = fallback
    row = market_recap.iloc[0].to_dict()
    cards = [
        ("加權指數", _format_cell("twse_index", row.get("twse_index"))),
        ("櫃買指數", _format_cell("tpex_index", row.get("tpex_index"))),
        ("市場環境分數", _format_cell("market_regime_score", row.get("market_regime_score"))),
        ("市場狀態", _format_cell("regime_label", row.get("regime_label"))),
        ("上漲家數", _format_cell("advancers", row.get("advancers"))),
        ("下跌家數", _format_cell("decliners", row.get("decliners"))),
        ("平盤家數", _format_cell("unchanged", row.get("unchanged"))),
        ("漲停家數", _format_cell("limit_up_count", row.get("limit_up_count"))),
        ("跌停家數", _format_cell("limit_down_count", row.get("limit_down_count"))),
    ]
    table = _table(market_recap, MARKET_RECAP_COLUMNS_FOR_TABLE, "目前尚無大盤復盤資料", 5)
    content = (
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + f'<p class="recap-summary">{escape(_format_cell("recap_summary", row.get("recap_summary")))}</p>'
        + f'<p class="note">{escape(_format_cell("data_quality_note", row.get("data_quality_note")))}</p>'
        + _details_block("大盤復盤原始資料", table)
    )
    return _section("大盤復盤", content)


def _benchmark_alpha_section(
    summary: dict[str, object],
    paper_summary: dict[str, object],
    recent_summaries: pd.DataFrame,
    market_regime: pd.DataFrame,
    market_recap: pd.DataFrame,
    sector_strength: pd.DataFrame,
) -> str:
    benchmark = _benchmark_snapshot(market_regime, market_recap, sector_strength)
    system_returns = _system_return_snapshot(summary, paper_summary, recent_summaries)
    headline_window = _best_alpha_window(system_returns, benchmark["returns"])
    system_headline = system_returns.get(headline_window)
    benchmark_headline = benchmark["returns"].get(headline_window)
    alpha = _alpha_return(system_headline, benchmark_headline)
    beat_text = _beat_market_text(alpha)
    alpha_text = _return_text(alpha)
    warning = ""
    if benchmark["warning"]:
        warning = (
            '<p class="top-notice benchmark-warning"><strong>Benchmark warning</strong>'
            f'<span>{escape(str(benchmark["warning"]))}</span></p>'
        )
    detail_rows = [
        ("今日", system_returns.get("1d"), benchmark["returns"].get("1d")),
        ("近 5 日", system_returns.get("5d"), benchmark["returns"].get("5d")),
        ("近 20 日", system_returns.get("20d"), benchmark["returns"].get("20d")),
        ("累計", system_returns.get("total"), benchmark["returns"].get("total")),
    ]
    detail_table = _benchmark_detail_table(detail_rows)
    content = (
        '<div class="benchmark-summary-grid">'
        + _benchmark_card("打敗大盤", beat_text, headline_window)
        + _benchmark_card("超額報酬 alpha", alpha_text, _benchmark_window_label(headline_window), alpha)
        + _benchmark_card("本系統總資產報酬率", _return_text(system_returns.get("total")), "相對初始資金")
        + _benchmark_card("Benchmark 報酬率", _return_text(benchmark_headline), str(benchmark["source_label"]))
        + "</div>"
        + warning
        + _details_block("大盤比較詳細數據", detail_table)
    )
    return _section("大盤比較 / 超額報酬", content, section_id="benchmark-alpha", class_name="benchmark-alpha-section")


def _market_regime_score_explainer(
    summary: dict[str, object],
    market_regime: pd.DataFrame,
    market_recap: pd.DataFrame,
) -> str:
    row = market_regime.iloc[0].to_dict() if not market_regime.empty else {}
    score = _first_raw(summary.get("market_regime_score"), row.get("market_regime_score"))
    fallback_used = _truthy(_frame_first(market_recap, "fallback_used")) or _truthy(row.get("fallback_used"))
    factors = [
        ("5 日市場報酬", _return_text(_normalized_return(row.get("market_return_5d")))),
        ("20 日市場報酬", _return_text(_normalized_return(row.get("market_return_20d")))),
        ("20 日均線站上比例", _ratio_or_bool_text(row.get("market_above_20ma_ratio"), row.get("twse_above_20ma"))),
        ("60 日均線站上比例", _ratio_or_bool_text(row.get("market_above_60ma_ratio"), row.get("twse_above_60ma"))),
        ("是否使用 fallback", "是" if fallback_used else "否"),
    ]
    content = (
        '<div class="regime-explainer">'
        '<div class="regime-definition">'
        f'<span>目前 market_regime_score</span><strong>{escape(_format_cell("market_regime_score", score))}</strong>'
        "<p>這是新增持倉風控分數，不是選股分數，也不是獲利保證；分數偏低時只代表新增紙上持倉要更保守。</p>"
        "</div>"
        '<div class="regime-factor-grid">'
        + "".join(_card(label, value) for label, value in factors)
        + "</div></div>"
    )
    return _section("market_regime_score 說明", content, section_id="market-regime-explainer", class_name="market-regime-explainer-section")


def _benchmark_snapshot(
    market_regime: pd.DataFrame,
    market_recap: pd.DataFrame,
    sector_strength: pd.DataFrame,
) -> dict[str, object]:
    regime_row = market_regime.iloc[0].to_dict() if not market_regime.empty else {}
    recap_uses_fallback = _truthy(_frame_first(market_recap, "fallback_used"))
    source = str(regime_row.get("source", "")).strip().lower()
    if source == "index" and not recap_uses_fallback:
        returns = {
            "1d": None,
            "5d": _normalized_return(regime_row.get("market_return_5d")),
            "20d": _normalized_return(regime_row.get("market_return_20d")),
            "total": None,
        }
        return {
            "source_label": "加權指數",
            "warning": "",
            "returns": returns,
        }
    if not sector_strength.empty and "stock_id" in sector_strength.columns:
        frame = sector_strength.copy()
        frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
        etf_0050 = frame[frame["stock_id"] == "0050"]
        if not etf_0050.empty:
            row = etf_0050.iloc[0].to_dict()
            return {
                "source_label": "0050 fallback",
                "warning": "未使用正式加權指數資料；本次 benchmark fallback 使用 0050，不能假裝是正式大盤指數。",
                "returns": {
                    "1d": None,
                    "5d": _normalized_return(row.get("stock_return_5d")),
                    "20d": _normalized_return(row.get("stock_return_20d")),
                    "total": None,
                },
            }
        market_return_rows = frame.dropna(subset=["market_return_5d", "market_return_20d"], how="all") if {"market_return_5d", "market_return_20d"}.issubset(frame.columns) else pd.DataFrame()
        if not market_return_rows.empty:
            row = market_return_rows.iloc[0].to_dict()
            return {
                "source_label": "全市場等權 fallback",
                "warning": "未使用正式加權指數資料；本次 benchmark fallback 使用全市場等權報酬。",
                "returns": {
                    "1d": None,
                    "5d": _normalized_return(row.get("market_return_5d")),
                    "20d": _normalized_return(row.get("market_return_20d")),
                    "total": None,
                },
            }
    return {
        "source_label": "benchmark 資料不足",
        "warning": "缺少正式加權指數、0050 與全市場等權資料，無法計算 benchmark alpha。",
        "returns": {"1d": None, "5d": None, "20d": None, "total": None},
    }


def _system_return_snapshot(
    summary: dict[str, object],
    paper_summary: dict[str, object],
    recent_summaries: pd.DataFrame,
) -> dict[str, float | None]:
    source = paper_summary or summary
    total_equity = _first_number(source, "total_equity_after_cost") or _first_number(source, "total_equity")
    total_capital = _first_number(source, "total_capital")
    total = (total_equity / total_capital - 1.0) if total_equity is not None and total_capital else None
    return {
        "1d": _equity_return_over_recent_window(recent_summaries, 1),
        "5d": _equity_return_over_recent_window(recent_summaries, 5),
        "20d": _equity_return_over_recent_window(recent_summaries, 20),
        "total": total,
    }


def _equity_return_over_recent_window(recent_summaries: pd.DataFrame, window: int) -> float | None:
    if recent_summaries.empty:
        return None
    equity_column = "total_equity_after_cost" if "total_equity_after_cost" in recent_summaries.columns else "total_equity"
    if equity_column not in recent_summaries.columns:
        return None
    frame = recent_summaries.copy()
    if "trade_date" in frame.columns:
        frame["_sort_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        frame = frame.sort_values("_sort_date", ascending=False)
    values = pd.to_numeric(frame[equity_column], errors="coerce").dropna().tolist()
    if len(values) <= window:
        return None
    latest, baseline = float(values[0]), float(values[window])
    if abs(baseline) < 0.000001:
        return None
    return latest / baseline - 1.0


def _best_alpha_window(system_returns: dict[str, float | None], benchmark_returns: dict[str, float | None]) -> str:
    for key in ["5d", "20d", "1d"]:
        if system_returns.get(key) is not None and benchmark_returns.get(key) is not None:
            return key
    return "total"


def _alpha_return(system_return: float | None, benchmark_return: float | None) -> float | None:
    if system_return is None or benchmark_return is None:
        return None
    return system_return - benchmark_return


def _beat_market_text(alpha: float | None) -> str:
    if alpha is None:
        return "資料不足"
    return "是" if alpha >= 0 else "否"


def _benchmark_card(label: str, value: str, note: str, raw_value: float | None = None) -> str:
    value_class = _profit_class(raw_value) if raw_value is not None else "profit-flat"
    return (
        '<article class="benchmark-card">'
        f"<span>{escape(label)}</span>"
        f'<strong class="{value_class}">{escape(value)}</strong>'
        f"<em>{escape(note)}</em>"
        "</article>"
    )


def _benchmark_detail_table(rows: list[tuple[str, float | None, float | None]]) -> str:
    body = []
    for label, system_value, benchmark_value in rows:
        alpha = _alpha_return(system_value, benchmark_value)
        body.append(
            "<tr>"
            f"<td>{escape(label)}</td>"
            f"<td>{escape(_return_text(system_value))}</td>"
            f"<td>{escape(_return_text(benchmark_value))}</td>"
            f'<td class="{_profit_class(alpha)}">{escape(_return_text(alpha))}</td>'
            f"<td>{escape(_beat_market_text(alpha))}</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap always-table"><table class="benchmark-detail-table">'
        "<thead><tr><th>期間</th><th>本系統報酬率</th><th>Benchmark 報酬率</th><th>Alpha</th><th>是否打敗大盤</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table></div>"
    )


def _benchmark_window_label(window: str) -> str:
    return {"1d": "今日", "5d": "近 5 日", "20d": "近 20 日", "total": "累計"}.get(window, window)


def _return_text(value: float | None) -> str:
    if value is None:
        return "資料不足"
    return f"{value * 100:+.2f}%"


def _normalized_return(value: object) -> float | None:
    number = _to_float(value)
    if number is None:
        return None
    if abs(number) > 0.5 and abs(number) <= 100:
        return number / 100.0
    if abs(number) > 100:
        return None
    return number


def _ratio_or_bool_text(ratio_value: object, bool_value: object) -> str:
    ratio = _to_float(ratio_value)
    if ratio is not None:
        if abs(ratio) <= 1:
            return f"{ratio * 100:.1f}%"
        if abs(ratio) <= 100:
            return f"{ratio:.1f}%"
    if _is_blank(bool_value):
        return "資料不足"
    return "是" if _truthy(bool_value) else "否"


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
            ("持倉風險燈號", _format_cell("risk_light", row.get("risk_light"))),
            ("持倉提示", _format_cell("holding_action_hint", row.get("holding_action_hint"))),
            ("剩餘股數", _format_cell("remaining_shares", row.get("remaining_shares") if not _is_blank(row.get("remaining_shares")) else row.get("shares"))),
            ("成交均價", _format_cell("entry_price", row.get("entry_price"))),
            ("最新價格", _format_cell("current_price", row.get("current_price"))),
            ("目前市值", _format_cell("market_value", row.get("market_value"))),
        ]
        metrics.extend(
            [
                ("流動性分數", _format_cell("liquidity_score", row.get("liquidity_score"))),
                ("產業相對強弱分數", _format_cell("sector_strength_score", row.get("sector_strength_score"))),
            ]
        )
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
            "risk_light",
            "holding_action_hint",
            "holding_risk_reason",
            "remaining_shares",
            "entry_price",
            "current_price",
            "market_value",
            "unrealized_pnl",
            "unrealized_pnl_pct",
            "stop_loss_price",
            "liquidity_score",
            "sector_strength_score",
        ],
        "目前尚無持倉",
        max_rows=50,
    )
    return (
        '<div class="broker-cards">' + "".join(cards) + "</div>"
        + _details_block("原始持倉資料表格", table, class_name="raw-table-details")
    )


def _enrich_with_local_factor_csv(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "stock_id" not in frame.columns:
        return frame
    output = frame.copy()
    local_sources = [
        (
            ROOT / "data" / "liquidity.csv",
            [
                "liquidity_score",
                "avg_turnover_20d",
                "latest_volume",
                "latest_turnover",
                "turnover_ratio_20d",
                "slippage_risk_score",
                "liquidity_warning",
            ],
        ),
        (
            ROOT / "data" / "sector_strength.csv",
            [
                "sector_strength_score",
                "relative_strength_5d",
                "relative_strength_20d",
                "sector_strength_rank",
                "sector_strength_warning",
            ],
        ),
    ]
    output["stock_id"] = output["stock_id"].astype(str).str.strip()
    for path, columns in local_sources:
        if not path.exists():
            continue
        source = _read_csv(path)
        if source.empty or "stock_id" not in source.columns:
            continue
        source = source.copy()
        source["stock_id"] = source["stock_id"].astype(str).str.strip()
        if "trade_date" in source.columns:
            source = source.sort_values("trade_date")
        lookup = source.drop_duplicates("stock_id", keep="last").set_index("stock_id")
        for column in columns:
            if column not in lookup.columns:
                continue
            mapped = output["stock_id"].map(lookup[column])
            if column in output.columns:
                output[column] = output[column].where(~output[column].apply(_is_blank), mapped)
            else:
                output[column] = mapped
    return output


def _apply_holding_risk_lights(frame: pd.DataFrame, config: dict[str, object]) -> pd.DataFrame:
    if frame.empty:
        return frame
    output = frame.copy()
    local_config = config.get("local_factors", {}) if isinstance(config, dict) else {}
    light_config = local_config.get("holding_risk_light", {}) if isinstance(local_config, dict) else {}
    near_stop_loss_pct = _to_float(light_config.get("near_stop_loss_pct")) or 0.03
    if light_config and not bool(light_config.get("enabled", True)):
        output["risk_light"] = "綠燈"
        output["holding_action_hint"] = "正常續抱"
        output["holding_risk_reason"] = "持倉風險燈號已停用"
        return output

    results = output.apply(lambda row: _holding_risk_light(row, near_stop_loss_pct), axis=1, result_type="expand")
    output[["risk_light", "holding_action_hint", "holding_risk_reason"]] = results
    return output


def _holding_risk_light(row: pd.Series, near_stop_loss_pct: float) -> tuple[str, str, str]:
    red_reasons: list[str] = []
    yellow_reasons: list[str] = []
    current_price = _to_float(row.get("current_price"))
    stop_loss = _to_float(row.get("stop_loss_price"))
    if current_price and stop_loss and current_price > 0:
        distance = (current_price - stop_loss) / current_price
        if distance <= near_stop_loss_pct:
            red_reasons.append("接近停損")
        elif distance <= 0.05:
            yellow_reasons.append("距停損 5% 內")
    if _truthy(row.get("is_disposition_stock")):
        red_reasons.append("處置股")
    if str(row.get("event_risk_level", "")).strip().upper() == "HIGH":
        red_reasons.append("高風險事件")
    risk_flags = str(row.get("risk_flags", "") or "")
    if "重大負面" in risk_flags:
        red_reasons.append("重大負面事件")
    liquidity_score = _to_float(row.get("liquidity_score"))
    if liquidity_score is not None and liquidity_score < 40:
        red_reasons.append("流動性偏低")
    if red_reasons:
        return "紅燈", "高風險 / 接近出場", "，".join(dict.fromkeys(red_reasons))

    confidence = _to_float(row.get("confidence_score"))
    sector_score = _to_float(row.get("sector_strength_score"))
    if confidence is not None and confidence < 60:
        yellow_reasons.append("市場情報資料可信度偏低")
    if liquidity_score is not None and 40 <= liquidity_score < 60:
        yellow_reasons.append("流動性普通")
    if sector_score is not None and sector_score < 45:
        yellow_reasons.append("產業 / 相對強弱偏弱")
    if _truthy(row.get("is_attention_stock")):
        yellow_reasons.append("注意股")
    for column in ["market_intel_warning", "financial_warning", "valuation_warning", "liquidity_warning", "sector_strength_warning"]:
        text = str(row.get(column, "") or "").strip()
        if text and text != "nan":
            yellow_reasons.append(text)
    if str(row.get("partial_exit_1_done", "")).strip().lower() in {"true", "1", "yes", "是"}:
        yellow_reasons.append("已部分停利，剩餘部位需觀察")
    if yellow_reasons:
        return "黃燈", "需人工留意", "，".join(dict.fromkeys(yellow_reasons))
    return "綠燈", "正常續抱", "未觸發出場，資料品質正常"


def _today_action_summary(
    summary: dict[str, object],
    pending_orders: pd.DataFrame,
    open_positions: pd.DataFrame,
    data_fetch_status: pd.DataFrame,
    trading_decisions: pd.DataFrame | None = None,
) -> str:
    items: list[str] = []
    if _uses_recent_data(summary):
        items.append("目前使用最近有效交易日資料，非即時交易日。")
    if str(summary.get("guardrail_status", "")).upper() == "BLOCKED" or _format_cell("new_entries_allowed", summary.get("new_entries_allowed")) in {"否", "False"}:
        reason = _format_cell("pause_new_entries_reason", summary.get("pause_new_entries_reason"))
        items.append(f"Paper guardrails 暫停新增持倉：{reason if reason != '-' else '需人工確認風控狀態'}。")
    regime_score = _to_float(summary.get("market_regime_score"))
    if regime_score is not None and 0 < regime_score < 60:
        items.append(f"市場環境分數 {regime_score:.0f} 低於新增持倉門檻，暫不新增紙上 pending order。")
    pending_count = 0
    if not pending_orders.empty and "status" in pending_orders.columns:
        pending_count = int((pending_orders["status"].fillna("").astype(str).str.upper() == "PENDING").sum())
    elif not pending_orders.empty:
        pending_count = len(pending_orders)
    if pending_count:
        items.append(f"有 {pending_count} 筆等待進場，請人工確認流動性與事件風險。")
    take_profit = int(_to_float(summary.get("take_profit_exits")) or 0)
    stop_loss = int(_to_float(summary.get("stop_loss_exits")) or 0)
    if take_profit or stop_loss:
        label = "最近有效交易日" if _uses_recent_data(summary) else "今日"
        items.append(f"{label}有 {take_profit} 筆停利、{stop_loss} 筆停損。")
    if not open_positions.empty and "risk_light" in open_positions.columns:
        lights = open_positions["risk_light"].fillna("").astype(str)
        red_count = int((lights == "紅燈").sum())
        yellow_count = int((lights == "黃燈").sum())
        if red_count or yellow_count:
            items.append(f"有 {red_count} 檔紅燈、{yellow_count} 檔黃燈持倉，需人工檢查。")
    if not data_fetch_status.empty and "source_name" in data_fetch_status.columns:
        local_status = data_fetch_status[data_fetch_status["source_name"].isin(["liquidity", "sector_strength"])]
        if not local_status.empty and local_status["status"].fillna("").astype(str).str.upper().isin(["OK", "OK_WITH_FALLBACK"]).all():
            items.append("流動性與相對強弱已由本地價量資料衍生，不依賴外部 API。")
        elif not local_status.empty:
            items.append("流動性或相對強弱資料不足，請以技術面與風控結果交叉確認。")
    if trading_decisions is not None and not trading_decisions.empty and "decision" in trading_decisions.columns:
        decisions = trading_decisions["decision"].fillna("").astype(str)
        buy_count = int((decisions == "BUY_CANDIDATE").sum())
        high_count = int(decisions.isin(["NO_TRADE", "EXIT"]).sum())
        if buy_count:
            items.append(f"決策引擎列出 {buy_count} 檔買進候選，僅供人工確認。")
        if high_count:
            items.append(f"決策引擎列出 {high_count} 檔高風險或出場檢查標的，需人工確認。")
    if not items:
        items.append("今日流程無重大異常，仍需人工檢查候選股理由與風控狀態。")
    safe_items = [
        item.replace("建議買進", "等待人工確認").replace("建議賣出", "等待人工確認")
        for item in items[:5]
    ]
    return '<ul class="action-list">' + "".join(f"<li>{escape(item)}</li>" for item in safe_items) + "</ul>"


def _decision_overview(summary: dict[str, object], decisions: pd.DataFrame) -> str:
    cards = [
        ("A 級候選股數", _format_cell("grade_a_count", summary.get("grade_a_count"))),
        ("B 級候選股數", _format_cell("grade_b_count", summary.get("grade_b_count"))),
        ("C 級候選股數", _format_cell("grade_c_count", summary.get("grade_c_count"))),
        ("D 級候選股數", _format_cell("grade_d_count", summary.get("grade_d_count"))),
        ("買進候選數", _format_cell("buy_candidate_count", summary.get("buy_candidate_count"))),
        ("觀察名單數", _format_cell("watch_only_count", summary.get("watch_only_count"))),
        ("不交易名單數", _format_cell("no_trade_count", summary.get("no_trade_count"))),
        ("HOLD 數", _format_cell("hold_count", summary.get("hold_count"))),
        ("REDUCE review 數", _format_cell("reduce_count", summary.get("reduce_count"))),
        ("EXIT review 數", _format_cell("exit_review_count", summary.get("exit_review_count"))),
    ]
    if not summary and not decisions.empty:
        counts = decisions["decision"].fillna("").astype(str).value_counts().to_dict()
        cards = [
            ("買進候選數", str(counts.get("BUY_CANDIDATE", 0))),
            ("觀察名單數", str(counts.get("WATCH_ONLY", 0))),
            ("不交易名單數", str(counts.get("NO_TRADE", 0))),
        ]
    note = '<p class="note">決策引擎僅供人工確認，未自動下單。</p>'
    return _section("決策引擎摘要", '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>" + note)


def _guardrail_overview(
    summary: dict[str, object],
    market_regime: pd.DataFrame,
    rejected_orders: pd.DataFrame,
) -> str:
    regime_score = summary.get("market_regime_score")
    if (_to_float(regime_score) or 0) == 0 and not market_regime.empty:
        regime_score = market_regime.iloc[0].get("market_regime_score")
    allow_entries = summary.get("new_entries_allowed")
    if _is_blank(allow_entries):
        allow_entries = "是"
    reason = _format_cell("pause_new_entries_reason", summary.get("pause_new_entries_reason"))
    cards = [
        ("市場環境分數", _format_cell("market_regime_score", regime_score)),
        ("是否允許新增持倉", _format_cell("new_entries_allowed", allow_entries)),
        ("Guardrail 狀態", _format_cell("guardrail_status", summary.get("guardrail_status"))),
        ("暫停新倉原因", reason),
        ("Active pending", _format_cell("pending_orders_active_count", summary.get("pending_orders_active_count"))),
        ("Executed pending", _format_cell("pending_orders_executed_count", summary.get("pending_orders_executed_count"))),
        ("Expired pending", _format_cell("pending_orders_expired_count", summary.get("pending_orders_expired_count"))),
        ("Cancelled pending", _format_cell("pending_orders_cancelled_count", summary.get("pending_orders_cancelled_count"))),
        ("訊號建立被擋", _format_cell("rejected_orders_signal_count", summary.get("rejected_orders_signal_count"))),
        ("執行前被擋", _format_cell("rejected_orders_execution_count", summary.get("rejected_orders_execution_count"))),
        ("被擋總數", _format_cell("rejected_orders_total_count", summary.get("rejected_orders_total_count") or summary.get("rejected_orders"))),
    ]
    rejected_table = _table(
        rejected_orders,
        [
            "stock_id",
            "stock_name",
            "candidate_grade",
            "signal_date",
            "status",
            "rejection_stage",
            "final_order_status",
            "attempted_execution_date",
            "order_age_trading_days",
            "market_regime_score",
            "rejection_reason",
            "rejected_reason",
        ],
        "目前尚無 guardrail 擋下的交易",
        max_rows=20,
    )
    expiry = load_config(ROOT / "config.yaml").get("pending_order", {}).get("expire_after_trading_days", 1)
    note = (
        '<p class="note">Guardrails 只影響紙上交易 pending order；不影響既有持倉出場，也不會真實下單。'
        f" Pending order 有效期限：{escape(str(expiry))} 個交易日。</p>"
    )
    return _section(
        "Paper trading guardrails",
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + note
        + _details_block("被擋下交易明細", rejected_table),
    )


def _enrichment_overview(summary: dict[str, object], enrichment: pd.DataFrame) -> str:
    ai_used = _count_true(enrichment, "ai_used")
    rule_based = _count_equal(enrichment, "enrichment_provider", "rule_based")
    insufficient = _count_in_set(enrichment, "enrichment_status", {"PARTIAL", "INSUFFICIENT_DATA"})
    if summary:
        ai_used = int(_to_float(summary.get("ai_used_count")) or ai_used)
        rule_based = int(_to_float(summary.get("rule_based_enrichment_count")) or rule_based)
        insufficient = int(_to_float(summary.get("enrichment_insufficient_data_count")) or insufficient)
    cards = [
        ("AI / Enrichment 狀態", _format_cell("ai_enrichment_status", summary.get("ai_enrichment_status"))),
        ("AI 使用筆數", f"{ai_used:,.0f}"),
        ("Rule-based fallback 筆數", f"{rule_based:,.0f}"),
        ("資料不足筆數", f"{insufficient:,.0f}"),
        ("產業分類補強狀態", _format_cell("industry_map_status", summary.get("industry_map_status"))),
    ]
    detail = _responsive_compact_records(
        enrichment,
        ["stock_id", "stock_name", "ai_summary", "manual_review_focus", "enrichment_status", "ai_used", "source_evidence_count"],
        ["risk_explanation", "opportunity_explanation", "data_quality_explanation", "valuation_context", "margin_credit_context", "sector_context", "source_evidence_json"],
        "目前尚無 AI / enrichment 資料",
        20,
    )
    note = '<p class="note">AI / enrichment 預設使用 rule-based，僅解釋既有資料；不會下單，也不承諾獲利。</p>'
    return _section("AI / Enrichment 摘要", '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>" + note + _details_block("AI / Enrichment 明細", detail))


def _evidence_table(evidence: pd.DataFrame) -> str:
    return _table(
        evidence,
        [
            "trade_date",
            "stock_id",
            "stock_name",
            "source_name",
            "source_type",
            "source_date",
            "field_name",
            "field_value",
            "evidence_summary",
            "fallback_used",
            "confidence_impact",
        ],
        "目前尚無資料來源依據",
        100,
    )


def _loss_attribution_overview(loss_attribution: pd.DataFrame) -> str:
    if loss_attribution.empty:
        return _section("Loss attribution 摘要", _empty("目前尚無虧損歸因資料"))
    realized = pd.to_numeric(
        loss_attribution.get("realized_pnl_pct", pd.Series([None] * len(loss_attribution))),
        errors="coerce",
    )
    unrealized = pd.to_numeric(
        loss_attribution.get("unrealized_pnl_pct", pd.Series([None] * len(loss_attribution))),
        errors="coerce",
    )
    returns = realized.where(realized.notna(), unrealized).fillna(0.0)
    loss_frame = loss_attribution[returns < 0].copy()
    top_reason = "-"
    if not loss_frame.empty and "likely_loss_reason" in loss_frame.columns:
        reasons = loss_frame["likely_loss_reason"].fillna("").astype(str)
        reasons = reasons[reasons.str.strip() != ""]
        if not reasons.empty:
            top_reason = reasons.value_counts().index[0]
    cards = [
        ("虧損交易數", f"{len(loss_frame):,.0f}"),
        ("最大不利波動最低值", _format_number_or_dash(pd.to_numeric(loss_attribution.get("max_adverse_excursion"), errors="coerce").min())),
        ("主要虧損原因", top_reason),
    ]
    table = _table(
        loss_attribution,
        [
            "stock_id",
            "stock_name",
            "candidate_grade",
            "decision",
            "realized_pnl_pct",
            "unrealized_pnl_pct",
            "exit_reason",
            "liquidity_score",
            "sector_strength_score",
            "confidence_score",
            "market_regime_score",
            "loss_bucket",
            "likely_loss_reason",
        ],
        "目前尚無虧損歸因資料",
        max_rows=50,
    )
    return _section(
        "Loss attribution 摘要",
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"
        + _details_block("Loss attribution 明細", table),
    )


def _decision_engine_content(decisions: pd.DataFrame, validation: pd.DataFrame) -> str:
    if decisions.empty:
        decision_summary = _empty("目前尚無交易決策資料")
    else:
        decision_summary = '<div class="cards">' + "".join(
            _card(label, value)
            for label, value in [
                ("A 級", f"{_decision_count(decisions, 'candidate_grade', 'A'):,.0f}"),
                ("B 級", f"{_decision_count(decisions, 'candidate_grade', 'B'):,.0f}"),
                ("C 級", f"{_decision_count(decisions, 'candidate_grade', 'C'):,.0f}"),
                ("D 級", f"{_decision_count(decisions, 'candidate_grade', 'D'):,.0f}"),
                ("BUY_CANDIDATE", f"{_decision_count(decisions, 'decision', 'BUY_CANDIDATE'):,.0f}"),
                ("WATCH_ONLY", f"{_decision_count(decisions, 'decision', 'WATCH_ONLY'):,.0f}"),
                ("NO_TRADE", f"{_decision_count(decisions, 'decision', 'NO_TRADE'):,.0f}"),
                ("HOLD", f"{_decision_count(decisions, 'decision', 'HOLD'):,.0f}"),
                ("REDUCE", f"{_decision_count(decisions, 'decision', 'REDUCE'):,.0f}"),
                ("EXIT review", f"{_decision_count(decisions, 'decision', 'EXIT'):,.0f}"),
            ]
        ) + "</div>"
    summary_columns = [
        "stock_id",
        "stock_name",
        "decision",
        "candidate_grade",
        "decision_level",
        "action",
        "confidence_score",
        "liquidity_score",
        "sector_strength_score",
        "can_auto_trade",
        "requires_manual_review",
        "review_level",
        "review_reason",
        "ai_summary",
        "manual_review_focus",
    ]
    detail_columns = [
        "trade_date",
        "source",
        "current_status",
        "reason",
        "risk_flags",
        "positive_signals",
        "warning_signals",
        "blocking_risks",
        "momentum_signal",
        "data_quality_flags",
        "investment_risk_flags",
        "total_score",
        "multi_factor_score",
        "final_market_score",
        "event_risk_level",
        "position_size_suggestion",
        "data_quality_note",
        "risk_explanation",
        "data_quality_explanation",
        "valuation_context",
        "valuation_risk_level",
        "margin_credit_context",
        "margin_risk_level",
        "margin_price_divergence",
        "industry_main",
        "industry_sub",
        "sector_strength_mode",
        "relative_strength_5d",
        "relative_strength_20d",
        "sector_context",
        "enrichment_status",
        "ai_used",
        "source_evidence_count",
        "source_evidence_json",
    ]
    sections = [
        _section("今日決策摘要", decision_summary + '<p class="note">所有決策皆為 advisory / paper-only，不會建立真實委託；can_auto_trade=false。</p>'),
        _section("A/B/C/D 分級統計", decision_summary),
        _section("BUY_CANDIDATE 清單", _responsive_compact_records(_decision_filter(decisions, "BUY_CANDIDATE"), summary_columns, detail_columns, "目前無買進候選", 10)),
        _section("WATCH_ONLY 清單", _responsive_compact_records(_decision_filter(decisions, "WATCH_ONLY"), summary_columns, detail_columns, "目前無觀察名單", 10)),
        _section("NO_TRADE 清單", _responsive_compact_records(_decision_filter(decisions, "NO_TRADE"), summary_columns, detail_columns, "目前無不交易名單", 10)),
        _section("持倉 HOLD / REDUCE / EXIT review 清單", _responsive_compact_records(_decision_filter_any(decisions, ["HOLD", "REDUCE", "EXIT"]), summary_columns, detail_columns, "目前無持倉決策", 20)),
        _details_block("完整 trading_decisions 原始表格", _table(decisions, [column for column in decisions.columns if column in COLUMN_LABELS], "目前無交易決策原始資料", 100)),
        _details_block("策略驗證報表", _table(validation, [column for column in validation.columns if column in COLUMN_LABELS], "目前尚無策略驗證資料", 20)),
    ]
    return "".join(sections)


def _decision_filter(decisions: pd.DataFrame, decision: str) -> pd.DataFrame:
    if decisions.empty or "decision" not in decisions.columns:
        return pd.DataFrame(columns=decisions.columns)
    return decisions[decisions["decision"].fillna("").astype(str) == decision].copy()


def _decision_filter_any(decisions: pd.DataFrame, values: list[str]) -> pd.DataFrame:
    if decisions.empty or "decision" not in decisions.columns:
        return pd.DataFrame(columns=decisions.columns)
    return decisions[decisions["decision"].fillna("").astype(str).isin(values)].copy()


def _decision_count(decisions: pd.DataFrame, column: str, value: str) -> int:
    if decisions.empty or column not in decisions.columns:
        return 0
    return int((decisions[column].fillna("").astype(str) == value).sum())


def _count_true(frame: pd.DataFrame, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].apply(_to_bool).sum())


def _count_equal(frame: pd.DataFrame, column: str, value: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int((frame[column].fillna("").astype(str) == value).sum())


def _count_in_set(frame: pd.DataFrame, column: str, values: set[str]) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].fillna("").astype(str).str.upper().isin(values).sum())


def _pending_cards(frame: pd.DataFrame) -> str:
    if frame.empty:
        return _empty("目前尚無待進場資料")
    statuses = frame["status"].fillna("").astype(str).str.upper() if "status" in frame.columns else pd.Series(["PENDING"] * len(frame))
    waiting = frame[statuses == "PENDING"].copy()
    expired = frame[statuses == "EXPIRED"].copy()
    cancelled = frame[statuses.str.startswith("CANCELLED_")].copy()
    skipped = frame[statuses.str.contains("SKIPPED|SKIP", regex=True)].copy()
    executed = frame[statuses == "EXECUTED"].copy()
    summary = (
        '<div class="cards">'
        + _card("等待進場", f"{len(waiting):,.0f}")
        + _card("Active pending", f"{len(waiting):,.0f}")
        + _card("Executed pending", f"{len(executed):,.0f}")
        + _card("Expired pending", f"{len(expired):,.0f}")
        + _card("Cancelled pending", f"{len(cancelled):,.0f}")
        + _card("已略過", f"{len(skipped):,.0f}")
        + "</div>"
    )
    waiting_cards = _pending_card_list(waiting, "目前尚無等待進場資料")
    expired_cards = _pending_card_list(expired, "目前尚無已過期待進場資料")
    cancelled_cards = _pending_card_list(cancelled, "目前尚無已取消待進場資料")
    skipped_cards = _pending_card_list(skipped, "目前尚無已略過進場資料")
    table = _table(
        frame,
        ["signal_date", "planned_entry_date", "actual_entry_date", "attempted_execution_date", "stock_id", "stock_name", "signal_close", "entry_price", "status", "order_age_trading_days", "expires_after_trading_days", "fundamental_score", "fundamental_reason", "skipped_reason", "rejection_reason"],
        "目前尚無待進場資料",
        max_rows=50,
    )
    return (
        summary
        + "<h3>等待進場</h3>"
        + waiting_cards
        + "<h3>已過期</h3>"
        + expired_cards
        + "<h3>已取消</h3>"
        + cancelled_cards
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
        summary_fields = _detail_grid(
            row,
            [
                "signal_date",
                "planned_entry_date",
                "status",
                "liquidity_score",
                "sector_strength_score",
                "final_market_score",
                "confidence_score",
                "risk_flags",
                "final_comment",
            ],
        )
        detail_fields = _detail_grid(
            row,
            [
                "actual_entry_date",
                "signal_close",
                "entry_price",
                "fundamental_score",
                "fundamental_reason",
                "institutional_score",
                "credit_score",
                "event_risk_score",
                "ai_summary",
                "manual_review_focus",
                "valuation_context",
                "margin_credit_context",
                "sector_context",
                "source_evidence_count",
                "source_evidence_json",
            ],
        )
        cards.append(
            '<article class="mobile-card pending-card">'
            f'<div class="card-title-row"><h3>{escape(stock_id)} {escape(stock_name)}</h3>'
            f'<span>{escape(_format_cell("status", row.get("status")))}</span></div>'
            f"{summary_fields}<details class=\"card-details\"><summary>資料來源依據與完整資料</summary>{detail_fields}</details></article>"
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
        "risk_light",
        "holding_action_hint",
        "holding_risk_reason",
        "avg_turnover_20d",
        "relative_strength_5d",
        "relative_strength_20d",
        "ai_summary",
        "manual_review_focus",
        "risk_explanation",
        "opportunity_explanation",
        "data_quality_explanation",
        "source_evidence_count",
        "enrichment_status",
        "ai_used",
        "pe_ratio",
        "pb_ratio",
        "dividend_yield",
        "valuation_context",
        "valuation_risk_level",
        "margin_credit_context",
        "margin_risk_level",
        "margin_price_divergence",
        "industry_main",
        "industry_sub",
        "sector_strength_mode",
        "sector_context",
        "source_evidence_json",
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
        "enrichment_status",
        "enrichment_provider",
        "ai_used",
        "source_evidence_count",
        "missing_data_flags",
        "enriched_industry",
        "enriched_industry_source",
        "industry_main",
        "industry_sub",
        "valuation_context",
        "valuation_risk_level",
        "margin_credit_context",
        "margin_risk_level",
        "margin_price_divergence",
        "sector_context",
        "sector_strength_mode",
        "relative_strength_5d",
        "relative_strength_20d",
        "risk_explanation",
        "opportunity_explanation",
        "data_quality_explanation",
        "manual_review_focus",
        "ai_summary",
        "ai_warning",
        "source_evidence_json",
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


def _refresh_data_quality_health(
    report_dir: Path,
    candidates: pd.DataFrame,
    data_fetch_status: pd.DataFrame,
) -> pd.DataFrame:
    try:
        write_data_quality_health(report_dir, candidates, data_fetch_status)
    except Exception:
        pass
    return _read_csv(report_dir / "data_quality_health.csv")


def _data_quality_health_section(data_quality_health: pd.DataFrame, section_id: str = "") -> str:
    if data_quality_health.empty:
        return _section("資料健康檢查", _empty("目前尚無 data_quality_health.csv"), section_id=section_id, class_name="data-quality-health")
    data_issues = _count_true(data_quality_health, "data_issue")
    investment_risks = _count_true(data_quality_health, "investment_risk")
    warning_count = _status_count(data_quality_health, "health_status", "WARNING")
    attention_count = _status_count(data_quality_health, "health_status", "ATTENTION")
    cards = [
        ("資料問題項目", f"{data_issues:,.0f}"),
        ("投資風險項目", f"{investment_risks:,.0f}"),
        ("警告項目", f"{warning_count:,.0f}"),
        ("注意項目", f"{attention_count:,.0f}"),
    ]
    table = _table(
        data_quality_health,
        [
            "check_name",
            "category",
            "health_status",
            "review_level",
            "review_reason",
            "data_issue",
            "investment_risk",
            "affected_symbols_count",
            "source_name",
            "status",
            "fallback_action",
        ],
        "目前尚無資料健康檢查資料",
        max_rows=40,
    )
    note = '<div class="note">資料不足只代表需要補查或使用 fallback，不等同真正投資風險；投資風險會獨立列在投資風險旗標與檢查原因。</div>'
    return _section(
        "資料健康檢查",
        '<div class="cards">' + "".join(_card(label, value) for label, value in cards) + "</div>" + note + table,
        section_id=section_id,
        class_name="data-quality-health",
    )


def _status_count(frame: pd.DataFrame, column: str, value: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(frame[column].fillna("").astype(str).str.upper().eq(value).sum())


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
        ("被擋下交易數", _format_cell("rejected_orders", summary.get("rejected_orders"))),
        ("市場環境分數", _format_cell("market_regime_score", summary.get("market_regime_score"))),
        ("是否允許新增持倉", _format_cell("new_entries_allowed", summary.get("new_entries_allowed"))),
        (f"{prefix} open positions 數量", _format_cell("open_positions", summary.get("open_positions"))),
        (f"{prefix} closed trades 數量", _format_cell("closed_positions", summary.get("closed_positions"))),
        (f"{prefix} market intelligence 狀態", _format_cell("market_intel_status", summary.get("market_intel_status"))),
        ("Guardrail 狀態", _format_cell("guardrail_status", summary.get("guardrail_status"))),
        ("資料品質摘要", _data_quality_summary(summary, data_fetch_status)),
    ]
    return '<div class="cards key-cards">' + "".join(_card(label, value) for label, value in cards) + "</div>"


def _data_quality_summary(summary: dict[str, object], data_fetch_status: pd.DataFrame) -> str:
    issues = _data_quality_issues(summary, data_fetch_status)
    if not issues:
        return "資料品質：正常"
    if _has_data_quality_warning(summary, data_fetch_status):
        return "資料品質：有警告"
    return "資料品質：有注意事項"


def _data_quality_detail_block(summary: dict[str, object], data_fetch_status: pd.DataFrame) -> str:
    issues = _data_quality_issues(summary, data_fetch_status)
    if not issues:
        issues = ["目前未偵測到重大資料品質問題"]
    items = "".join(f"<li>{escape(issue)}</li>" for issue in issues)
    return _details_block("資料品質詳細說明", f'<ul class="quality-list">{items}</ul>')


def _data_quality_issues(summary: dict[str, object], data_fetch_status: pd.DataFrame) -> list[str]:
    if not summary:
        return ["缺少每日 summary"]
    issues: list[str] = []
    if str(summary.get("status", "")).upper() == "FAILED" or not _is_blank(summary.get("error_message")):
        error = _format_cell("error_message", summary.get("error_message"))
        issues.append(_humanize_top_error(error if error != "-" else "流程執行失敗"))
    if (_to_float(summary.get("market_intel_warning_count")) or 0) > 0:
        issues.append("市場情報資料不足，未影響流程")
    if _market_intel_is_stale(summary, pd.DataFrame()):
        issues.append("市場資料過期，不建議短線進場。")
    elif str(summary.get("fallback_reason", "")).strip() == "no trading data" and _has_market_freshness_metadata(summary):
        issues.append("非交易日，使用最近交易日資料。")
    if str(summary.get("market_intel_status", "")).upper() == "CACHE":
        issues.append("市場情報使用快取資料")
    if not data_fetch_status.empty and "status" in data_fetch_status.columns:
        for _, row in data_fetch_status.iterrows():
            issue = _data_source_quality_issue(row)
            if issue and issue not in issues:
                issues.append(issue)
    return issues


def _has_data_quality_warning(summary: dict[str, object], data_fetch_status: pd.DataFrame) -> bool:
    if not summary:
        return True
    if str(summary.get("status", "")).upper() == "FAILED" or not _is_blank(summary.get("error_message")):
        return True
    if data_fetch_status.empty or "status" not in data_fetch_status.columns:
        return False
    for _, row in data_fetch_status.iterrows():
        status = str(row.get("status", "")).strip().upper()
        fallback_action = str(row.get("fallback_action", "")).strip()
        source = str(row.get("source_name", "")).strip()
        if status in {"FAILED", "MISSING"} and fallback_action != "kept_existing_csv" and source != "monthly_revenue":
            return True
    return False


def _data_source_quality_issue(row: pd.Series) -> str:
    source = str(row.get("source_name", "")).strip()
    status = str(row.get("status", "")).strip().upper()
    fallback_action = str(row.get("fallback_action", "")).strip()
    fallback_reason = str(row.get("fallback_reason", "")).strip()
    freshness_level = str(row.get("data_freshness_level", "")).strip().upper()
    warning = str(row.get("warning", "")).strip()
    error_message = str(row.get("error_message", "")).strip()
    if source == "market_intel":
        if freshness_level in {"STALE", "CACHE"} or status == "CACHE":
            return "市場資料過期，不建議短線進場。"
        if fallback_reason == "no trading data":
            return "非交易日，使用最近交易日資料。"
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
        warning = "" if _data_source_warning_text(row) == "無" else _data_source_warning_text(row)
        error_message = "" if _safe_text(row.get("error_message")) == "無" else _safe_text(row.get("error_message"))
        health_status = _provider_health_status_from_row(row)
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


def _non_data_source_health_items(items: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    return [item for item in items if not item[0].startswith("資料來源：")]


def _data_source_summary_section(data_fetch_status: pd.DataFrame) -> str:
    return _section("資料來源摘要表", _data_source_summary_table(data_fetch_status), class_name="data-source-summary")


def _data_source_summary_table(data_fetch_status: pd.DataFrame) -> str:
    if data_fetch_status.empty:
        return _empty("目前尚無 data_fetch_status 資料")

    rows = []
    for _, row in data_fetch_status.iterrows():
        status_text = str(row.get("status", "")).strip().upper()
        rows.append(
            [
                _source_display_name(row.get("source_name")),
                _provider_health_status_from_row(row),
                _data_source_plain_description(row),
                _format_source_rows(row.get("rows")),
                _data_source_impact(row),
            ]
        )
    return _plain_table(["資料源", "狀態", "人話說明", "筆數", "影響程度"], rows, class_name="summary-table source-summary-table")


def _data_source_technical_details(data_fetch_status: pd.DataFrame) -> str:
    if data_fetch_status.empty:
        return _empty("目前尚無資料來源技術細節")
    rows = []
    for _, row in data_fetch_status.iterrows():
        rows.append(
            [
                _safe_text(row.get("source_name")),
                _safe_text(row.get("provider_maturity")),
                _safe_text(row.get("status")),
                _safe_text(row.get("fallback_action")),
                _truncate_text(_safe_text(row.get("warning")), 220),
                _truncate_text(_safe_text(row.get("error_message")), 220),
            ]
        )
    return _plain_table(
        ["source_name", "provider_maturity", "status", "fallback_action", "warning", "error_message"],
        rows,
        class_name="technical-table",
    )


def _plain_table(headers: list[str], rows: list[list[str]], class_name: str = "") -> str:
    if not rows:
        return _empty("目前尚無資料")
    class_attr = f' class="{escape(class_name)}"' if class_name else ""
    header = "".join(f"<th>{escape(header_text)}</th>" for header_text in headers)
    body = []
    for row in rows:
        cells = "".join(f"<td>{escape(_safe_text(value))}</td>" for value in row)
        body.append(f"<tr>{cells}</tr>")
    return f'<div class="table-wrap always-table"><table{class_attr}><thead><tr>{header}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def _source_display_name(value: object) -> str:
    source = _safe_text(value)
    overrides = {
        "monthly_revenue": "月營收",
        "institutional": "三大法人",
        "margin_short": "融資融券",
        "valuation": "估值",
        "financials": "財報",
        "attention_disposition": "注意 / 處置股",
        "material_events": "重大訊息",
        "sector_strength": "產業 / 相對強弱",
        "liquidity": "流動性",
        "market_intel": "市場情報",
    }
    if source in overrides:
        return overrides[source]
    return {
        "monthly_revenue": "月營收",
        "institutional": "三大法人",
        "margin_short": "融資融券",
        "valuation": "估值",
        "financials": "財報",
        "attention_disposition": "注意 / 處置股",
        "material_events": "重大訊息",
        "sector_strength": "產業相對強弱",
        "liquidity": "流動性",
    }.get(source, source)


def _data_source_plain_description(row: pd.Series) -> str:
    source = str(row.get("source_name", "")).strip()
    status = str(row.get("status", "")).strip().upper()
    rows = int(_to_float(row.get("rows")) or 0)
    maturity = str(row.get("provider_maturity", "")).strip().lower()
    fallback_action = str(row.get("fallback_action", "")).strip()
    error_message = str(row.get("error_message", "")).strip()
    freshness_level = str(row.get("data_freshness_level", "")).strip().upper()
    if source == "market_intel" and (freshness_level in {"STALE", "CACHE"} or status == "CACHE"):
        return "市場資料過期或使用快取資料"
    if maturity == "local_derived" and status in {"OK", "OK_WITH_FALLBACK"}:
        return "本地價量衍生資料，不依賴外部 API"
    if source == "monthly_revenue" and ("HTTPError: 404" in error_message or "404 Client Error" in error_message):
        return "尚未取得新資料，已保留既有資料"
    if fallback_action == "kept_existing_csv" or status == "OK_WITH_FALLBACK":
        return "尚未取得新資料，已保留既有資料"
    if status == "CACHE":
        return "使用快取資料"
    if status == "OK" and rows > 0:
        return "已取得資料"
    if status in {"EMPTY", "MISSING"}:
        return "尚未取得資料，採中性或既有資料"
    if status == "FAILED":
        return "資料來源失敗，已使用 fallback 或採中性"
    if maturity in {"placeholder", "csv_fallback"}:
        return "尚未接正式來源，採中性或既有資料"
    return "未提供"


def _data_source_impact(row: pd.Series) -> str:
    status = str(row.get("status", "")).strip().upper()
    fallback_action = str(row.get("fallback_action", "")).strip()
    source = str(row.get("source_name", "")).strip()
    freshness_level = str(row.get("data_freshness_level", "")).strip().upper()
    if source == "market_intel" and (freshness_level in {"STALE", "CACHE"} or status == "CACHE"):
        return "阻擋短線買進候選"
    if status == "OK" and fallback_action != "kept_existing_csv":
        return "正常"
    if source == "monthly_revenue" or fallback_action == "kept_existing_csv" or status in {"CACHE", "EMPTY", "OK_WITH_FALLBACK"}:
        return "不影響流程"
    if status in {"FAILED", "MISSING"}:
        return "需人工確認"
    return "不影響流程"


def _format_source_rows(value: object) -> str:
    number = _to_float(value)
    if number is None:
        return "未提供"
    return f"{number:,.0f}"


def _truncate_text(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def _safe_text(value: object) -> str:
    if _is_blank(value):
        return "無"
    text = str(value).strip()
    return "無" if text.lower() == "nan" or text == "" else text


def _data_source_warning_text(row: pd.Series) -> str:
    source = str(row.get("source_name", "")).strip()
    error_message = "" if _safe_text(row.get("error_message")) == "無" else _safe_text(row.get("error_message"))
    if source == "monthly_revenue" and ("HTTPError: 404" in error_message or "404 Client Error" in error_message):
        return "月營收資料尚未發布或來源暫不可用，已保留既有資料。"
    warning = _safe_text(row.get("warning"))
    return "" if warning == "無" else warning


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


def _provider_health_status_from_row(row: pd.Series) -> str:
    fallback_action = str(row.get("fallback_action", "")).strip()
    if fallback_action == "kept_existing_csv":
        return "注意"
    return _provider_health_status(
        str(row.get("status", "")).strip().upper(),
        int(_to_float(row.get("rows")) or 0),
        str(row.get("provider_maturity", "")).strip(),
    )


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
    warnings: list[str] = []
    notices: list[str] = []
    infos: list[str] = []
    for name, status, detail in items:
        if _is_top_notice(name, status, detail):
            notices.append(_top_notice_message(name, detail))
        elif _is_top_warning(name, status, detail):
            warnings.append(_top_warning_message(name, detail))
        elif _is_top_info(name, status, detail):
            infos.append(_top_info_message(name, detail))

    blocks = []
    for level, title, messages in [
        ("warning", "警告", warnings),
        ("notice", "注意", notices),
        ("info", "資訊", infos),
    ]:
        unique_messages = list(dict.fromkeys(message for message in messages if message))
        if unique_messages:
            blocks.append(
                f'<div class="top-{level}"><strong>{escape(title)}</strong><span>{escape("；".join(unique_messages))}</span></div>'
            )
    return "".join(blocks)


def _is_top_warning(name: str, status: str, detail: str) -> bool:
    if status != "警告":
        return False
    if name.startswith("資料來源："):
        return False
    return name in {
        "最新有效交易日",
        "paper_trades.csv",
        "reports/index.html",
        "data update",
        "candidate export",
        "position update",
        "report generation",
    }


def _is_top_notice(name: str, status: str, detail: str) -> bool:
    if name.startswith("資料來源：monthly_revenue") and "已保留既有資料" in detail:
        return True
    return False


def _is_top_info(name: str, status: str, detail: str) -> bool:
    return False


def _top_notice_message(name: str, detail: str) -> str:
    if "資料來源：monthly_revenue" in name:
        return "月營收資料尚未取得，已保留既有資料，不影響今日流程。"
    return f"{name}：{_strip_urls(detail)}"


def _top_info_message(name: str, detail: str) -> str:
    return f"{name}：{_strip_urls(detail)}"


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
    using_cache = _market_intel_using_cache(summary, frame, data_fetch_status)
    stale_notice = _market_intel_stale_notice(summary, frame)
    cards = [
        ("市場情報來源", source or "-"),
        ("是否為 mock", "是" if is_mock else "否"),
        ("是否使用 cache", "是" if using_cache else "否"),
        ("市場情報要求資料日", _market_intel_requested_date(summary, frame)),
        ("市場情報實際資料日", _market_intel_actual_data_date(summary, frame)),
        ("市場情報替代原因", _market_intel_fallback_reason(summary, frame)),
        ("市場情報快取 / 資料年齡", _market_intel_cache_age_text(frame)),
        ("市場情報資料鮮度", _market_intel_freshness_level(summary, frame)),
        ("市場情報是否過期資料", "是" if _market_intel_is_stale(summary, frame) else "否"),
        ("市場情報資料不足股票數", _format_cell("market_intel_warning_count", summary.get("market_intel_warning_count"))),
        ("基本面資料不足股票數", f"{_fundamental_missing_count(candidates):,.0f}"),
        ("估值資料不足股票數", f"{_reason_missing_count(candidates, 'valuation_score', 'valuation_reason'):,.0f}"),
        ("財報資料不足股票數", f"{_reason_missing_count(candidates, 'financial_score', 'financial_reason'):,.0f}"),
        ("月營收資料狀態", _source_status_summary(data_fetch_status, "monthly_revenue")),
        ("三大法人資料狀態", _source_status_summary(data_fetch_status, "institutional")),
        ("融資融券資料狀態", _source_status_summary(data_fetch_status, "margin_short")),
        ("注意 / 處置股資料狀態", _source_status_summary(data_fetch_status, "attention_disposition")),
    ]
    cards.extend(
        [
            ("正式資料覆蓋率", _coverage_summary(data_fetch_status)),
            ("真實資料來源數", f"{_real_source_count(data_fetch_status):,.0f}"),
            ("mock 資料股票數", f"{_mock_symbol_count(frame):,.0f}"),
            ("使用最近可用月營收資料月份", _source_latest_period(data_fetch_status, "monthly_revenue")),
        ]
    )
    notes = []
    if is_mock:
        notes.append("目前為 mock / 中性資料，尚未接入正式新聞來源，不應視為完整新聞 / 財報分析。")
        notes.append("新聞來源狀態：尚未接入")
    if stale_notice:
        notes.append(stale_notice)
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


def _market_intel_stale_notice(summary: dict[str, object], frame: pd.DataFrame) -> str:
    status = str(summary.get("market_intel_status", "") or "").strip().upper()
    fallback_reason = str(summary.get("fallback_reason", "") or "").strip()
    freshness_level = _market_intel_freshness_level(summary, frame)
    stale = status == "CACHE" or freshness_level in {"STALE", "CACHE"}
    if not frame.empty:
        if "market_intel_status" in frame.columns:
            stale = stale or frame["market_intel_status"].fillna("").astype(str).str.upper().eq("CACHE").any()
        if "is_stale_data" in frame.columns:
            stale = stale or frame["is_stale_data"].apply(_truthy).any()
    requested = _market_intel_requested_date(summary, frame)
    actual = _market_intel_actual_data_date(summary, frame)
    reason = _market_intel_fallback_reason(summary, frame)
    age = _market_intel_cache_age_text(frame)
    if stale:
        return (
            "市場資料過期，不建議短線進場；目前市場情報使用快取或非當日資料，不建議短線自動進場。"
            f"要求日期 {requested}，實際資料日 {actual}，原因 {reason}，資料年齡 {age}。"
        )
    if fallback_reason == "no trading data" or (
        not frame.empty
        and "fallback_reason" in frame.columns
        and frame["fallback_reason"].fillna("").astype(str).eq("no trading data").any()
    ):
        return f"非交易日，使用最近交易日資料。要求日期 {requested}，實際資料日 {actual}。"
    return ""


def _market_intel_is_stale(summary: dict[str, object], frame: pd.DataFrame) -> bool:
    if _truthy(summary.get("is_stale_data")):
        return True
    if _market_intel_freshness_level(summary, frame) in {"STALE", "CACHE"}:
        return True
    if frame.empty or "is_stale_data" not in frame.columns:
        return False
    return bool(frame["is_stale_data"].apply(_truthy).any())


def _market_intel_freshness_level(summary: dict[str, object], frame: pd.DataFrame) -> str:
    for value in [summary.get("data_freshness_level"), _frame_first(frame, "data_freshness_level")]:
        if _is_blank(value):
            continue
        text = str(value).strip().upper()
        if text and text != "-":
            return text
    return "UNKNOWN"


def _market_intel_using_cache(
    summary: dict[str, object],
    frame: pd.DataFrame,
    data_fetch_status: pd.DataFrame,
) -> bool:
    if str(summary.get("market_intel_status", "")).upper() == "CACHE":
        return True
    if not frame.empty and "market_intel_status" in frame.columns:
        if frame["market_intel_status"].fillna("").astype(str).str.upper().eq("CACHE").any():
            return True
    if data_fetch_status.empty:
        return False
    market_rows = data_fetch_status
    if "source_name" in market_rows.columns:
        market_rows = market_rows[market_rows["source_name"].fillna("").astype(str) == "market_intel"]
    if market_rows.empty:
        return False
    if "status" in market_rows.columns and market_rows["status"].fillna("").astype(str).str.upper().eq("CACHE").any():
        return True
    if "fallback_action" in market_rows.columns and market_rows["fallback_action"].fillna("").astype(str).eq("cache").any():
        return True
    return False


def _has_market_freshness_metadata(summary: dict[str, object]) -> bool:
    return not _is_blank(summary.get("data_freshness_level")) or not _is_blank(summary.get("actual_data_date"))


def _market_intel_requested_date(summary: dict[str, object], frame: pd.DataFrame) -> str:
    return _first_non_blank(summary.get("requested_date"), _frame_first(frame, "requested_date"), summary.get("trade_date"))


def _market_intel_actual_data_date(summary: dict[str, object], frame: pd.DataFrame) -> str:
    return _first_non_blank(_frame_first(frame, "actual_data_date"), summary.get("trade_date"))


def _market_intel_fallback_date(summary: dict[str, object], frame: pd.DataFrame) -> str:
    return _first_non_blank(summary.get("fallback_date"), _frame_first(frame, "fallback_date"))


def _market_intel_fallback_reason(summary: dict[str, object], frame: pd.DataFrame) -> str:
    return _first_non_blank(summary.get("fallback_reason"), _frame_first(frame, "fallback_reason"), "-")


def _market_intel_cache_age_text(frame: pd.DataFrame) -> str:
    if frame.empty or "cache_age_days" not in frame.columns:
        return "-"
    values = pd.to_numeric(frame["cache_age_days"], errors="coerce").dropna()
    if values.empty:
        return "-"
    return f"{int(values.max()):,.0f} 天"


def _frame_first(frame: pd.DataFrame, column: str) -> object:
    if frame.empty or column not in frame.columns:
        return ""
    values = [value for value in frame[column].tolist() if not _is_blank(value)]
    return values[0] if values else ""


def _first_non_blank(*values: object) -> str:
    for value in values:
        if not _is_blank(value):
            return _format_cell("status", value)
    return "-"


def _first_raw(*values: object) -> object:
    for value in values:
        if not _is_blank(value):
            return value
    return ""


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


def _coverage_summary(data_fetch_status: pd.DataFrame) -> str:
    if data_fetch_status.empty or "is_real_data" not in data_fetch_status.columns:
        return "尚無資料"
    real = data_fetch_status["is_real_data"].apply(_truthy).sum()
    total = len(data_fetch_status)
    if total == 0:
        return "尚無資料"
    return f"{real / total:.0%}（{real}/{total}）"


def _real_source_count(data_fetch_status: pd.DataFrame) -> int:
    if data_fetch_status.empty or "is_real_data" not in data_fetch_status.columns:
        return 0
    return int(data_fetch_status["is_real_data"].apply(_truthy).sum())


def _mock_symbol_count(frame: pd.DataFrame) -> int:
    if frame.empty or "market_intel_source" not in frame.columns:
        return 0
    return int(frame["market_intel_source"].fillna("").astype(str).str.lower().eq("mock").sum())


def _source_latest_period(data_fetch_status: pd.DataFrame, source_name: str) -> str:
    if data_fetch_status.empty or "source_name" not in data_fetch_status.columns:
        return "-"
    matches = data_fetch_status[data_fetch_status["source_name"].fillna("").astype(str) == source_name]
    if matches.empty:
        return "-"
    row = matches.iloc[0]
    for column in ["latest_available_period", "actual_period", "requested_period"]:
        value = row.get(column)
        if not _is_blank(value):
            return str(value)
    return "-"


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}


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
    stale_notice = _market_intel_stale_notice(summary, frame)
    negative_news = 0
    if not is_mock and "news_sentiment_score" in frame.columns:
        negative_news = int((pd.to_numeric(frame["news_sentiment_score"], errors="coerce").fillna(0) < 0).sum())
    top_score = _format_cell("final_market_score", summary.get("market_intel_top_score"))
    cards = [
        ("市場判斷狀態", _format_cell("market_intel_status", summary.get("market_intel_status"))),
        ("市場判斷來源", source),
        ("要求資料日", _market_intel_requested_date(summary, frame)),
        ("實際資料日", _market_intel_actual_data_date(summary, frame)),
        ("替代交易日", _market_intel_fallback_date(summary, frame)),
        ("替代原因", _market_intel_fallback_reason(summary, frame)),
        ("快取 / 資料年齡", _market_intel_cache_age_text(frame)),
        ("資料鮮度等級", _market_intel_freshness_level(summary, frame)),
        ("是否過期資料", "是" if _market_intel_is_stale(summary, frame) else "否"),
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
        "requested_date",
        "actual_data_date",
        "fallback_date",
        "fallback_reason",
        "cache_age_days",
        "is_stale_data",
        "data_freshness_level",
    ]
    detail = _responsive_records(frame, columns, "今日無市場判斷資料", 20)
    note = ""
    if is_mock:
        note = '<div class="note">目前為 mock / 中性資料，尚未接入正式新聞來源，不應視為完整新聞 / 財報分析。</div>'
    if stale_notice:
        note += f'<div class="note">{escape(stale_notice)}</div>'
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
    today_exits = _today_exit_frame(closed_trades, open_positions, summary.get("trade_date") if summary else None)
    complete_exit_count = _count_exit_type(today_exits, "完整出場")
    partial_exit_count = _count_exit_type(today_exits, "部分停利 / 部分出場")
    cards = [
        (f"{prefix}停利筆數", _format_cell("take_profit_exits", summary.get("take_profit_exits"))),
        (f"{prefix}停損筆數", _format_cell("stop_loss_exits", summary.get("stop_loss_exits"))),
        (f"{prefix}移動停利筆數", _format_cell("trailing_stop_exits", summary.get("trailing_stop_exits"))),
        (f"{prefix}趨勢出場筆數", _format_cell("trend_exit_exits", summary.get("trend_exit_exits"))),
        (f"{prefix}完整出場筆數", f"{complete_exit_count:,.0f}"),
        (f"{prefix}部分出場筆數", f"{partial_exit_count:,.0f}"),
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


def _count_exit_type(frame: pd.DataFrame, exit_type: str) -> int:
    if frame.empty or "exit_type" not in frame.columns:
        return 0
    return int((frame["exit_type"].fillna("").astype(str) == exit_type).sum())


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


def _config_summary(config: dict[str, object]) -> str:
    if not config:
        return _empty("目前尚無配置資料")

    def pick(section: str, key: str) -> object:
        value = config.get(section, {})
        return value.get(key, "") if isinstance(value, dict) else ""

    rows = [
        ["auto_trading.enabled", _raw_bool(pick("auto_trading", "enabled")), "真實下單總開關，預設關閉"],
        ["auto_trading.can_place_real_orders", _raw_bool(pick("auto_trading", "can_place_real_orders")), "禁止真實券商委託"],
        ["auto_trading.require_manual_approval", _raw_bool(pick("auto_trading", "require_manual_approval")), "任何未來實盤都需人工確認"],
        ["paper_trading_guardrails.enabled", _raw_bool(pick("paper_trading_guardrails", "enabled")), "紙上交易新增/執行前風控"],
        ["pending_order.expire_after_trading_days", _format_cell("expires_after_trading_days", pick("pending_order", "expire_after_trading_days")), "pending order 有效期限"],
        ["paper_trading_guardrails.max_open_positions", _format_cell("open_positions", pick("paper_trading_guardrails", "max_open_positions")), "最大紙上持倉數"],
        ["paper_trading_guardrails.max_daily_new_positions", _format_cell("new_positions", pick("paper_trading_guardrails", "max_daily_new_positions")), "每日最多新增紙上持倉"],
        ["market_intel.enabled", _raw_bool(pick("market_intel", "enabled")), "市場情報是否啟用"],
        ["market_intel.provider", _format_cell("market_intel_source", pick("market_intel", "provider")), "台股市場情報 provider"],
        ["market_intel.affect_trading", _raw_bool(pick("market_intel", "affect_trading")), "市場情報不直接下單"],
        ["market_intel.cache_enabled", _raw_bool(pick("market_intel", "cache_enabled")), "市場情報快取"],
        ["market_intel.allow_mock", _raw_bool(pick("market_intel", "allow_mock")), "完全無資料時才允許 mock fallback"],
        ["multi_factor.affect_ranking", _raw_bool(pick("multi_factor", "affect_ranking")), "多因子預設不改候選排序"],
        ["multi_factor.affect_risk_pass", _raw_bool(pick("multi_factor", "affect_risk_pass")), "多因子預設不改 risk pass"],
        ["event_risk.block_disposition_stock", _raw_bool(pick("event_risk", "block_disposition_stock")), "處置股風控"],
        ["event_risk.block_attention_stock", _raw_bool(pick("event_risk", "block_attention_stock")), "注意股風控"],
        ["exit_strategy.take_profit_1_pct", _format_rate_percent(pick("exit_strategy", "take_profit_1_pct")), "第一段停利門檻"],
        ["exit_strategy.take_profit_2_pct", _format_rate_percent(pick("exit_strategy", "take_profit_2_pct")), "第二段停利門檻"],
        ["exit_strategy.trailing_stop_activate_pct", _format_rate_percent(pick("exit_strategy", "trailing_stop_activate_pct")), "移動停利啟動門檻"],
        ["exit_strategy.trailing_stop_drawdown_pct", _format_rate_percent(pick("exit_strategy", "trailing_stop_drawdown_pct")), "移動停利回落幅度"],
        ["exit_strategy.ma_exit_window", _format_cell("holding_days", pick("exit_strategy", "ma_exit_window")), "均線出場視窗"],
        ["exit_strategy.max_holding_days", _format_cell("holding_days", pick("exit_strategy", "max_holding_days")), "最長持有天數"],
        ["trading_cost.commission_rate", _format_permille(pick("trading_cost", "commission_rate")), "手續費率"],
        ["trading_cost.min_commission", _format_amount_plain(pick("trading_cost", "min_commission")), "最低手續費"],
        ["trading_cost.sell_tax_rate_stock", _format_rate_percent(pick("trading_cost", "sell_tax_rate_stock")), "股票交易稅"],
        ["trading_cost.sell_tax_rate_etf", _format_rate_percent(pick("trading_cost", "sell_tax_rate_etf")), "ETF 交易稅"],
        ["trading_cost.slippage_rate", _format_rate_percent(pick("trading_cost", "slippage_rate")), "滑價假設"],
        ["ai_enrichment.enabled", _raw_bool(pick("ai_enrichment", "enabled")), "資料解釋層"],
        ["ai_enrichment.provider", _format_cell("enrichment_provider", pick("ai_enrichment", "provider")), "預設 rule-based"],
        ["ai_enrichment.allow_external_ai", _raw_bool(pick("ai_enrichment", "allow_external_ai")), "外部 AI 預設關閉"],
        ["ai_enrichment.advisory_only", _raw_bool(pick("ai_enrichment", "advisory_only")), "只做輔助解釋"],
    ]
    table = _plain_table(["設定", "目前值", "說明"], rows, class_name="config-table")
    note = (
        '<p class="note">目前沒有真實下單能力；AI 只做資料解釋；market_intel 不直接下單；'
        "guardrails 只會阻擋紙上交易新倉或 pending execution。</p>"
    )
    return table + note


def _raw_bool(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip().lower()
    if text in {"true", "false"}:
        return text
    return _format_cell("status", value)


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


def _brief_recent_summaries(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    result = pd.DataFrame(index=frame.index)
    result["summary_requested_date"] = frame.get("requested_date", "")
    result["summary_trade_date"] = frame.get("trade_date", "")
    result["summary_status"] = frame.get("status", "")
    result["summary_candidate_rows"] = frame.get("candidate_rows", "")
    result["summary_risk_pass_rows"] = frame.get("risk_pass_rows", "")
    result["summary_pending_orders"] = frame.get("pending_orders", "")
    result["summary_executed_orders"] = frame.get("executed_orders", "")
    result["summary_open_positions"] = frame.get("open_positions", "")
    result["summary_closed_positions"] = frame.get("closed_positions", "")
    result["summary_total_equity_after_cost"] = frame.get("total_equity_after_cost", frame.get("total_equity", ""))
    result["summary_data_status"] = frame.get("multi_factor_data_status", frame.get("market_intel_status", frame.get("status", "")))
    return result


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
    header = "".join(f"<th>{escape(COLUMN_LABELS.get(column, column))}</th>" for column in visible_columns)
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


def _responsive_compact_records(
    frame: pd.DataFrame,
    summary_columns: list[str],
    detail_columns: list[str],
    empty_message: str,
    max_rows: int,
) -> str:
    table = _table(frame, summary_columns, empty_message, max_rows)
    if frame.empty:
        return table
    summary_visible = [column for column in summary_columns if column in frame.columns]
    detail_visible = [column for column in detail_columns if column in frame.columns and column not in summary_visible]
    cards = []
    for _, row in frame.head(max_rows).iterrows():
        title_parts = [
            _format_cell("stock_id", row.get("stock_id")),
            _format_cell("stock_name", row.get("stock_name")),
        ]
        title = " ".join(part for part in title_parts if part != "-")
        summary_fields = []
        for column in summary_visible:
            if column in {"stock_id", "stock_name"}:
                continue
            summary_fields.append(
                f"<dt>{escape(COLUMN_LABELS.get(column, column))}</dt><dd>{escape(_format_cell(column, row.get(column)))}</dd>"
            )
        details = _detail_grid(row, detail_visible)
        cards.append(
            '<article class="mobile-card market-card">'
            f'<h3>{escape(title or "候選股")}</h3>'
            f'<dl class="detail-grid compact-grid">{"".join(summary_fields)}</dl>'
            f'<details class="card-details"><summary>展開完整資料</summary>{details}</details>'
            "</article>"
        )
    return '<div class="mobile-cards">' + "".join(cards) + "</div>" + table


def _format_cell(column: str, value: object) -> str:
    if _is_blank(value):
        return "-"
    if str(value).strip() == "NEXT_AVAILABLE_TRADING_DAY":
        return "下一個有效交易日"

    if column == "entry_price_source":
        text = str(value).strip()
        return ENTRY_PRICE_SOURCE_LABELS.get(text, text)
    if column == "source_evidence_json":
        return _truncate_text(str(value), 600)
    if column == "category":
        text = str(value).strip()
        return {"source": "資料來源", "candidate": "候選股", "industry": "產業分類"}.get(text, text)
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


def _read_benchmark_sector_strength(report_dir: Path) -> pd.DataFrame:
    candidates = [
        report_dir / "sector_strength.csv",
        report_dir.parent / "data" / "sector_strength.csv",
    ]
    for path in candidates:
        frame = _read_csv(path)
        if not frame.empty:
            return frame
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
document.querySelectorAll('[data-section-target]').forEach(function(button){
  button.addEventListener('click', function(){
    var tab = button.getAttribute('data-tab-jump');
    var sectionId = button.getAttribute('data-section-target');
    var tabButton = document.querySelector('[data-tab-target="' + tab + '"]');
    if (tabButton) {
      tabButton.click();
    }
    window.setTimeout(function(){
      var section = document.getElementById(sectionId);
      if (section) {
        section.scrollIntoView({block: 'start'});
      }
    }, 0);
  });
});
"""


def _css() -> str:
    return """
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{margin:0;background:#080d18;color:#e5e7eb;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans TC",sans-serif;line-height:1.6}
.page{width:min(1440px,100%);margin:0 auto;padding:14px}
.account-header{padding:18px 2px 12px}
.account-header p{margin:0 0 4px;color:#38bdf8;font-size:13px;font-weight:700}
.account-header h1{margin:0 0 10px;font-size:26px;letter-spacing:0}
.account-header small{display:block;margin-top:10px;color:var(--text-muted);font-size:12px}
.header-meta{display:flex;gap:8px;overflow-x:auto;padding-bottom:2px}
.header-meta span{flex:0 0 auto;padding:5px 9px;border:1px solid #243244;border-radius:999px;background:#0f172a;color:#cbd5e1;font-size:12px}
.section-tabs{position:sticky;top:0;z-index:5;display:flex;gap:8px;overflow-x:auto;margin:10px -14px 12px;padding:9px 14px;background:rgba(8,13,24,.94);backdrop-filter:blur(10px);border-bottom:1px solid #1f2937}
.tab-button{flex:0 0 auto;padding:8px 12px;border:1px solid #243244;border-radius:999px;background:#111827;color:#dbeafe;font:700 14px/1.2 inherit;cursor:pointer}
.tab-button.active{background:#2563eb;border-color:#60a5fa;color:#fff}
.quick-section-nav{position:sticky;top:54px;z-index:4;display:flex;gap:8px;overflow-x:auto;margin:0 -14px 12px;padding:8px 14px;background:rgba(11,18,32,.92);border-bottom:1px solid #243244;backdrop-filter:blur(10px)}
.quick-nav-link{flex:0 0 auto;border:1px solid #243244;border-radius:999px;background:#0f172a;color:#cbd5e1;padding:7px 10px;font:700 12px/1.2 inherit;cursor:pointer}
.quick-nav-link:hover{border-color:#60a5fa;color:#fff;background:#1e293b}
.tab-panel{display:none}
.tab-panel.active{display:block}
section{margin:12px 0;padding:14px;background:#101827;border:1px solid #1f2937;border-radius:10px}
h2{margin:0 0 12px;font-size:18px;letter-spacing:0}
h3{margin:0 0 10px;font-size:16px;color:#f8fafc;letter-spacing:0}
.dashboard-overview-section{background:linear-gradient(180deg,#111827,#0b1220);border-color:#334155}
.dashboard-alert{margin:0 0 12px;padding:12px 14px;border-radius:10px;font-weight:800}
.dashboard-alert.ok{background:#052e2b;border:1px solid #0f766e;color:#ccfbf1}
.dashboard-alert.danger{background:#4c1d14;border:1px solid #f97316;color:#ffedd5}
.kpi-grid{display:grid;grid-template-columns:1fr;gap:12px}
.kpi-card{min-height:176px;padding:14px;border:1px solid #263244;border-radius:10px;background:#0b1220;display:flex;flex-direction:column;gap:10px}
.kpi-card h3{margin:0;color:#cbd5e1;font-size:13px}
.kpi-primary{display:flex;align-items:center;gap:8px;flex-wrap:wrap;color:#f8fafc;font-size:24px;font-weight:800;line-height:1.25}
.kpi-meta{display:grid;gap:7px;margin-top:auto}
.kpi-meta span{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;border-top:1px solid #1f2937;padding-top:7px}
.kpi-meta b{color:var(--text-muted);font-size:12px;font-weight:700}
.kpi-meta em{color:#e5e7eb;font-style:normal;text-align:right;font-size:12px;max-width:62%;word-break:break-word}
.kpi-card.ok{border-color:#0f766e}.kpi-card.warning{border-color:#b45309}.kpi-card.danger{border-color:#dc2626}.kpi-card.info{border-color:#2563eb}
.status-badge{display:inline-flex;align-items:center;justify-content:center;border-radius:999px;border:1px solid #334155;padding:5px 9px;font-size:12px;font-weight:900;line-height:1.1;white-space:nowrap}
.badge-ok{background:#064e3b;border-color:#10b981;color:#d1fae5}
.badge-info{background:#0c4a6e;border-color:#38bdf8;color:#e0f2fe}
.badge-warning{background:#713f12;border-color:#f59e0b;color:#fef3c7}
.badge-danger{background:#7f1d1d;border-color:#ef4444;color:#fee2e2}
.badge-neutral{background:#334155;border-color:#475569;color:#e2e8f0}
.status-strip{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
.decision-lanes{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-bottom:12px}
.decision-lane{padding:12px;border:1px solid #243244;border-radius:10px;background:#0b1220}
.decision-lane span,.decision-lane em{display:block;color:var(--text-muted);font-size:12px;font-style:normal}
.decision-lane strong{display:block;margin:5px 0;font-size:26px;color:#f8fafc}
.decision-buy{border-color:#14b8a6}.decision-watch{border-color:#38bdf8}.decision-no-trade{border-color:#64748b}.decision-hold{border-color:#22c55e}.decision-reduce{border-color:#f59e0b}.decision-exit{border-color:#ef4444}
.pnl-card{padding:14px;border:1px solid #334155;border-radius:12px;background:linear-gradient(180deg,#172033,#0f172a)}
.pnl-primary{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.pnl-secondary,.cards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:10px}
.overview-metric,.card{padding:12px;background:#0b1220;border:1px solid #243244;border-radius:10px}
.overview-metric span,.card span{display:block;color:var(--text-muted);font-size:12px}
.overview-metric strong,.card strong{display:block;margin-top:4px;font-size:17px;color:#f8fafc;word-break:break-word}
.overview-metric.pnl-main strong{font-size:24px}
.overview-metric.total-value strong{font-size:22px}
.chart-grid,.dashboard-split{display:grid;gap:12px}
.chart-card{padding:13px;background:#0b1220;border:1px solid #243244;border-radius:10px}
.pnl-bar-row{display:grid;grid-template-columns:110px 1fr 92px;gap:8px;align-items:center;margin:9px 0}
.pnl-bar-row span{font-size:12px;color:var(--text-muted)}.pnl-bar-row strong{text-align:right;font-size:13px}
.pnl-bar-track{height:10px;border-radius:999px;background:#1f2937;overflow:hidden}
.pnl-bar-track i{display:block;height:100%;border-radius:999px;background:#e5e7eb}
.pnl-bar-track i.profit-positive{background:#ef4444}.pnl-bar-track i.profit-negative{background:#22c55e}.pnl-bar-track i.profit-flat{background:var(--text-muted)}
.pnl-line-chart{width:100%;min-height:160px;background:#0f172a;border:1px solid #1f2937;border-radius:8px}
.chart-legend{display:flex;flex-wrap:wrap;gap:10px;margin-top:8px;color:#cbd5e1;font-size:12px}
.chart-legend i{display:inline-block;width:10px;height:10px;border-radius:999px;margin-right:5px}
.decision-stat-cards{margin-bottom:12px}
.risk-list,.catalyst-list{margin:0;padding-left:20px}
.risk-list li,.catalyst-list li{margin:7px 0}
.recap-summary{margin:10px 0 0;color:#e5e7eb;font-weight:700}
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
.position-pnl span,.closed-pnl span{display:block;color:var(--text-muted);font-size:12px}
.position-pnl strong,.closed-pnl strong{display:block;font-size:30px;font-weight:800;letter-spacing:0}
.position-pnl em{display:block;font-style:normal;font-size:15px;font-weight:700}
.pnl-highlight{min-height:96px;display:flex;flex-direction:column;justify-content:center}
.holding-metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
.holding-metrics div{padding:10px;border-radius:8px;background:#0b1220;border:1px solid #1f2937}
.holding-metrics span{display:block;color:var(--text-muted);font-size:12px}
.holding-metrics strong{font-size:15px;color:#f8fafc}
.card-details{margin-top:12px;border-top:1px solid #243244;padding-top:10px}
.compact-grid{grid-template-columns:120px 1fr}
.collapse-block{margin-top:12px;border:1px solid #243244;border-radius:10px;background:#0b1220}
.collapse-block>summary{padding:12px 13px;color:#bfdbfe;font-size:14px;font-weight:800;cursor:pointer;list-style:none}
.collapse-block>summary::-webkit-details-marker{display:none}
.collapse-block>summary:after{content:"展開";float:right;color:var(--text-muted);font-size:12px;font-weight:700}
.collapse-block[open]>summary:after{content:"收合"}
.collapse-content{padding:0 12px 12px}
summary{cursor:pointer;color:#bfdbfe;font-size:14px;font-weight:700}
.detail-grid{display:grid;grid-template-columns:120px 1fr;gap:7px 10px;margin:10px 0 0}
.detail-grid dt{color:var(--text-muted);font-size:12px}.detail-grid dd{margin:0;color:#e5e7eb;font-size:13px}
.table-wrap{display:none;width:100%;overflow-x:auto;border:1px solid #243244;border-radius:8px;margin-top:12px}
.table-wrap.always-table{display:block}
.collapse-block .table-wrap{display:block}
table{width:100%;border-collapse:collapse;min-width:760px;background:#0f172a}
th,td{padding:10px 12px;border-bottom:1px solid #243244;text-align:left;vertical-align:top}
th{color:#bae6fd;background:#172033;font-size:13px;white-space:nowrap}
td{font-size:13px;color:#e5e7eb;white-space:nowrap}
.summary-table td:nth-child(3),.summary-table td:nth-child(5),.technical-table td:nth-child(5),.technical-table td:nth-child(6){white-space:normal;min-width:220px}
tr:last-child td{border-bottom:0}
.empty,.note{padding:13px;background:#0f172a;border:1px solid #243244;border-radius:10px;color:#cbd5e1}
.note{border-color:#164e63;background:#082f49;margin-top:10px}
.quality-list{margin:0;padding-left:20px;color:#cbd5e1}
.action-list{margin:0;padding-left:20px;color:#e5e7eb}
.action-list li{margin:7px 0;line-height:1.55}
.risk-light-badge{border-radius:999px;padding:5px 9px;background:#334155;color:#e5e7eb}
.risk-light-badge.綠燈{background:#065f46;color:#d1fae5}
.risk-light-badge.黃燈{background:#854d0e;color:#fef3c7}
.risk-light-badge.紅燈{background:#991b1b;color:#fee2e2}
.top-info,.top-notice,.top-warning{display:flex;gap:10px;align-items:flex-start;margin:10px 0;padding:12px;border-radius:10px}
.top-info strong,.top-notice strong,.top-warning strong{white-space:nowrap}
.top-info{background:#0c4a6e;border:1px solid #38bdf8;color:#e0f2fe}
.top-notice{background:#713f12;border:1px solid #f59e0b;color:#fef3c7}
.top-warning{background:#7f1d1d;border:1px solid #ef4444;color:#fee2e2}
.health-grid{display:grid;grid-template-columns:1fr;gap:10px}
.health{padding:12px;background:#0f172a;border:1px solid #243244;border-radius:10px}
.health strong{display:inline-block;margin-right:8px;padding:2px 8px;border-radius:999px;font-size:12px}
.health span,.health em{display:block;margin-top:5px;font-style:normal}
.health.正常 strong{background:#065f46;color:#d1fae5}.health.注意 strong{background:#854d0e;color:#fef3c7}.health.警告 strong{background:#991b1b;color:#fee2e2}
@media(min-width:760px){.page{padding:22px}.account-header h1{font-size:32px}.section-tabs{margin:12px 0 16px;padding:10px 0}.quick-section-nav{margin:0 0 16px;padding:8px 0}.kpi-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.decision-lanes{grid-template-columns:repeat(6,minmax(0,1fr))}.pnl-primary{grid-template-columns:repeat(4,minmax(0,1fr))}.pnl-secondary,.cards{grid-template-columns:repeat(auto-fit,minmax(160px,1fr))}.chart-grid,.dashboard-split{grid-template-columns:repeat(2,minmax(0,1fr))}.holding-main{grid-template-columns:220px 1fr}.broker-cards,.mobile-cards{grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}.health-grid{grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}}

/* Brokerage-style dashboard refresh: distinct from any broker brand, focused on dense scanning. */
:root{--text-main:#182230;--text-secondary:#344054;--text-muted:#667085;--text-strong:#0b1220}
body{background:#edf2f7;color:var(--text-main);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans TC",sans-serif}
.page{width:min(1480px,100%);padding:14px 16px 28px}
.brokerage-header{display:grid;gap:16px;margin:0 0 14px;padding:18px;background:#ffffff;border:1px solid #d9e2ec;border-radius:8px;box-shadow:0 10px 24px rgba(15,23,42,.07)}
.brokerage-title-block p{margin:0 0 5px;color:#0f766e;font-size:12px;font-weight:900;letter-spacing:.04em;text-transform:uppercase}
.brokerage-title-block h1{margin:0;color:var(--text-strong);font-size:28px;font-weight:900}
.brokerage-title-block small{display:block;margin-top:8px;color:var(--text-secondary);font-size:12px}
.header-status-board{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;overflow:visible;padding:0}
.header-status-board .header-status-tile{display:block;min-width:0;flex:1 1 auto;padding:10px 12px;border:1px solid #d9e2ec;border-radius:7px;background:#f8fafc;color:var(--text-main);font-size:12px}
.header-status-board .header-status-tile b{display:block;color:var(--text-secondary);font-size:11px;font-weight:850}
.header-status-board .header-status-tile strong{display:block;margin-top:3px;color:var(--text-strong);font-size:14px;font-weight:900;word-break:break-word}
.section-tabs{position:sticky;top:0;z-index:20;margin:0 -16px 10px;padding:8px 16px;background:#ffffff;border-bottom:1px solid #d9e2ec;box-shadow:0 8px 20px rgba(15,23,42,.06);backdrop-filter:none}
.tab-button{border:0;border-radius:7px;background:#f1f5f9;color:var(--text-secondary);padding:9px 12px;font-size:13px;font-weight:900}
.tab-button.active{background:#0f766e;color:#ffffff;box-shadow:0 8px 18px rgba(15,118,110,.22)}
.quick-section-nav{display:none}
.quick-nav-link{border:1px solid #d9e2ec;border-radius:7px;background:#ffffff;color:var(--text-secondary);font-weight:800}
.quick-nav-link:hover{border-color:#0f766e;background:#ecfdf5;color:#0f766e}
.tab-panel{margin:0;padding:0;background:transparent;border:0;border-radius:0}
.tab-panel>h2{margin:0 0 12px;color:var(--text-strong);font-size:18px;font-weight:900}
section:not(.tab-panel){margin:12px 0;padding:16px;background:#ffffff;border:1px solid #d9e2ec;border-radius:8px;box-shadow:0 10px 24px rgba(15,23,42,.055)}
h2,h3{color:var(--text-strong)}
.dashboard-overview-section{background:#ffffff;border-color:#cbd5e1}
.dashboard-alert{border-radius:8px}
.dashboard-alert.ok{background:#ecfdf5;border:1px solid #86efac;color:#166534}
.dashboard-alert.danger{background:#fff7ed;border:1px solid #fdba74;color:#9a3412}
.kpi-grid{gap:10px}
.kpi-card{min-height:150px;border:1px solid #d9e2ec;border-top:4px solid #64748b;border-radius:8px;background:#ffffff;box-shadow:0 8px 20px rgba(15,23,42,.045)}
.kpi-card h3{color:var(--text-secondary);font-size:12px;font-weight:900}
.kpi-primary{color:var(--text-strong);font-size:22px}
.kpi-meta span{border-top:1px solid #edf2f7}
.kpi-meta b{color:var(--text-secondary);font-weight:850}
.kpi-meta em{color:var(--text-main);font-weight:750}
.kpi-card.ok{border-color:#d9e2ec;border-top-color:#16a34a}
.kpi-card.warning{border-color:#d9e2ec;border-top-color:#f59e0b}
.kpi-card.danger{border-color:#d9e2ec;border-top-color:#dc2626}
.kpi-card.info{border-color:#d9e2ec;border-top-color:#2563eb}
.status-badge{border-radius:6px;border:1px solid #d0d5dd;padding:5px 8px;font-size:11px}
.badge-ok{background:#dcfce7;border-color:#86efac;color:#166534}
.badge-info{background:#dbeafe;border-color:#93c5fd;color:#1d4ed8}
.badge-warning{background:#fef3c7;border-color:#fbbf24;color:#92400e}
.badge-danger{background:#fee2e2;border-color:#fca5a5;color:#991b1b}
.badge-neutral{background:#f1f5f9;border-color:#cbd5e1;color:var(--text-secondary)}
.profit-positive{color:#dc2626!important}.profit-negative{color:#047857!important}.profit-flat{color:var(--text-main)!important}
.positive{color:#dc2626!important}.negative{color:#047857!important}.neutral{color:var(--text-main)!important}
.decision-lane,.overview-metric,.card,.chart-card,.mobile-card,.health,.collapse-block{background:#ffffff;border-color:#d9e2ec;border-radius:8px}
.decision-lane strong,.overview-metric strong,.card strong,.holding-metrics strong{color:var(--text-strong);font-weight:900}
.decision-lane span,.decision-lane em,.overview-metric span,.card span,.holding-metrics span,.detail-grid dt{color:var(--text-secondary);font-weight:800}
.pnl-overview-section{border-color:#b7c7db;background:#ffffff}
.pnl-overview-layout{display:grid;gap:12px}
.asset-donut-card,.pnl-card{background:#ffffff;border:1px solid #cbd5e1;border-radius:8px;box-shadow:0 12px 26px rgba(15,23,42,.07)}
.asset-donut-card{padding:16px}
.asset-donut-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px;margin-bottom:12px}
.asset-donut-head h3{margin:0;color:var(--text-strong);font-size:17px;font-weight:900}
.asset-donut-head span{color:var(--text-secondary);font-size:12px;font-weight:800;text-align:right}
.asset-donut-body{display:grid;gap:16px;align-items:center}
.asset-visual-stack{display:grid;gap:12px;justify-items:center}
.asset-donut{width:min(258px,78vw);aspect-ratio:1;border-radius:50%;margin:0 auto;background:conic-gradient(#2f80ed 0 var(--holding-pct),#e6eef8 var(--holding-pct) 100%);display:grid;place-items:center;box-shadow:inset 0 0 0 1px #d9e2ec,0 14px 30px rgba(47,128,237,.14)}
.asset-donut-core{width:62%;height:62%;border-radius:50%;background:#ffffff;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;box-shadow:0 0 0 1px #d9e2ec}
.asset-donut-core span{color:var(--text-secondary);font-size:13px;font-weight:850}
.asset-donut-core strong{margin-top:6px;color:var(--text-strong);font-size:28px;font-weight:950;letter-spacing:0}
.asset-donut-core em{margin-top:6px;font-style:normal;font-size:14px;font-weight:900}
.asset-allocation{display:grid;gap:9px}
.allocation-row{display:flex;align-items:center;justify-content:space-between;gap:10px;color:var(--text-main);font-weight:850}
.allocation-row span{display:flex;align-items:center;gap:8px;color:var(--text-secondary)}
.asset-dot{width:12px;height:12px;border-radius:999px;display:inline-block}
.holding-dot{background:#2f80ed}.cash-dot{background:#b8c6d9}
.asset-allocation h4{margin:8px 0 0;color:var(--text-main);font-size:13px;font-weight:900}
.allocation-list{display:grid;gap:7px;margin:0;padding:0;list-style:none}
.allocation-list li{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:8px 0;border-top:1px solid #edf2f7}
.allocation-list b{display:block;color:var(--text-strong);font-size:13px}
.allocation-list em{display:block;color:var(--text-muted);font-size:12px;font-style:normal}
.allocation-list strong{color:var(--text-strong);font-size:14px;font-weight:900}
.asset-pnl-bottom{display:grid;grid-template-columns:1fr;gap:8px;width:100%;border-top:1px solid #edf2f7;padding-top:12px}
.asset-bottom-metric{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:9px 10px;background:#f8fafc;border:1px solid #d9e2ec;border-radius:7px}
.asset-bottom-metric span{color:var(--text-secondary);font-size:12px;font-weight:850}
.asset-bottom-metric strong{font-size:18px;font-weight:950}
.asset-donut-fallback .note{margin:0 0 10px}
.compact-empty{padding:10px;font-size:12px}
.benchmark-alpha-section,.market-regime-explainer-section{border-color:#cbd5e1;background:#ffffff}
.benchmark-summary-grid{display:grid;grid-template-columns:1fr;gap:10px}
.benchmark-card{padding:13px 14px;border:1px solid #cbd5e1;border-radius:8px;background:#f8fafc}
.benchmark-card span{display:block;color:var(--text-secondary);font-size:12px;font-weight:850}
.benchmark-card strong{display:block;margin-top:4px;color:var(--text-strong);font-size:24px;font-weight:950}
.benchmark-card em{display:block;margin-top:4px;color:var(--text-secondary);font-style:normal;font-size:12px;font-weight:750}
.benchmark-warning{margin-top:12px}
.benchmark-detail-table td:nth-child(4),.benchmark-detail-table td:nth-child(5){font-weight:900}
.regime-explainer{display:grid;gap:12px}
.regime-definition{padding:14px;border:1px solid #cbd5e1;border-radius:8px;background:#f8fafc}
.regime-definition span{display:block;color:var(--text-secondary);font-size:12px;font-weight:850}
.regime-definition strong{display:block;margin-top:5px;color:var(--text-strong);font-size:30px;font-weight:950}
.regime-definition p{margin:8px 0 0;color:var(--text-main);font-weight:750;line-height:1.6}
.regime-factor-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
.pnl-card{padding:14px;border-color:#cbd5e1;border-radius:8px;box-shadow:0 10px 24px rgba(15,23,42,.05)}
.pnl-kpi-panel h3{font-size:17px}
.pnl-kpi-panel .pnl-primary{grid-template-columns:repeat(2,minmax(0,1fr))}
.pnl-kpi-panel .overview-metric strong{word-break:normal}
.position-pnl,.closed-pnl,.holding-metrics div,.empty,.note{background:#f8fafc;border-color:#d9e2ec;color:#344054}
.note{background:#eef6ff;border-color:#bfdbfe}
.table-wrap{border-color:#d9e2ec;background:#ffffff}
table{background:#ffffff}
th{background:#f8fafc;color:var(--text-secondary);font-size:12px;font-weight:900;border-bottom:1px solid #d9e2ec}
td{color:var(--text-main);border-bottom:1px solid #edf2f7}
tbody tr:nth-child(even) td{background:#fbfdff}
.collapse-block>summary{color:var(--text-main);background:#f8fafc;border-radius:8px}
.collapse-content{background:#ffffff;border-radius:0 0 8px 8px}
.top-info{background:#eff6ff;border-color:#93c5fd;color:#1d4ed8}
.top-notice{background:#fffbeb;border-color:#fcd34d;color:#92400e}
.top-warning{background:#fff1f2;border-color:#fda4af;color:#9f1239}
@media(min-width:760px){.brokerage-header{grid-template-columns:minmax(260px,1.2fr) minmax(420px,2fr)}.header-status-board{grid-template-columns:repeat(4,minmax(0,1fr))}.kpi-grid{grid-template-columns:repeat(3,minmax(0,1fr))}.pnl-overview-layout{grid-template-columns:minmax(420px,.95fr) minmax(520px,1.05fr);align-items:stretch}.asset-donut-body{grid-template-columns:minmax(230px,300px) 1fr}.asset-pnl-bottom{grid-template-columns:repeat(2,minmax(0,1fr))}.asset-bottom-metric{display:block}.asset-bottom-metric strong{display:block;margin-top:4px}.benchmark-summary-grid{grid-template-columns:repeat(4,minmax(0,1fr))}.regime-explainer{grid-template-columns:minmax(260px,.8fr) minmax(420px,1.2fr)}.regime-factor-grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(min-width:1120px){
  .page{width:auto;max-width:none;margin:0;padding:24px 30px 36px 286px}
  .section-tabs{position:fixed;inset:0 auto 0 0;width:256px;height:100vh;display:flex;flex-direction:column;align-items:stretch;gap:6px;margin:0;padding:22px 14px;background:#182230;border:0;border-right:1px solid #263244;box-shadow:8px 0 24px rgba(15,23,42,.18)}
  .section-tabs:before{content:"TW-Quant";display:block;margin:0 8px 18px;color:#ffffff;font-size:20px;font-weight:900}
  .section-tabs:after{content:"Paper Trading Dashboard";display:block;margin:auto 8px 0;color:#98a2b3;font-size:11px;font-weight:800;text-transform:uppercase}
  .tab-button{width:100%;text-align:left;background:transparent;color:#cbd5e1;border-radius:7px;padding:11px 12px}
  .tab-button.active{background:#0f766e;color:#ffffff;box-shadow:none}
  .tab-button:hover{background:#263244;color:#ffffff}
  .quick-section-nav{display:none}
  .brokerage-header{position:relative}
}
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
