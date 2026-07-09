"""Advisory trading decision engine.

The engine writes review-only decisions. It never creates orders, changes
paper trades, or enables real trading.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd

from tw_quant.config import load_config
from tw_quant.decision.grading import apply_candidate_grades, grade_candidate


DECISION_COLUMNS = [
    "decision_date",
    "trade_date",
    "stock_id",
    "stock_name",
    "source",
    "current_status",
    "decision",
    "decision_level",
    "action",
    "candidate_grade",
    "reason",
    "risk_flags",
    "positive_signals",
    "warning_signals",
    "blocking_risks",
    "momentum_signal",
    "confidence_score",
    "total_score",
    "multi_factor_score",
    "final_market_score",
    "liquidity_score",
    "sector_strength_score",
    "event_risk_level",
    "position_size_suggestion",
    "can_auto_trade",
    "requires_manual_review",
    "review_level",
    "review_reason",
    "data_quality_flags",
    "investment_risk_flags",
    "data_quality_note",
]


@dataclass(frozen=True)
class TradingDecisionResult:
    trade_date: pd.Timestamp | None
    decisions: pd.DataFrame
    output_path: Path | None
    warning: str = ""


def generate_trading_decisions(
    reports_dir: str | Path = "reports",
    config_path: str | Path = "config.yaml",
    trade_date: str | None = None,
) -> TradingDecisionResult:
    report_dir = Path(reports_dir)
    config = load_config(config_path)
    candidates_path = _latest_file(report_dir, "candidates_*.csv", trade_date)
    if candidates_path is None:
        return TradingDecisionResult(None, pd.DataFrame(columns=DECISION_COLUMNS), None, "no candidates report found")

    candidates = _read_csv(candidates_path)
    paper_trades = _read_csv(report_dir / "paper_trades.csv")
    if candidates.empty:
        selected_date = _date_from_path(candidates_path)
        decisions = pd.DataFrame(columns=DECISION_COLUMNS)
        output_path = _write_decisions(report_dir, selected_date, decisions)
        return TradingDecisionResult(selected_date, decisions, output_path, "candidate report is empty")

    candidates = apply_candidate_grades(candidates)
    selected_date = pd.to_datetime(candidates["trade_date"].iloc[0], errors="coerce")
    if pd.isna(selected_date):
        selected_date = _date_from_path(candidates_path) or pd.Timestamp.today()
    decision_rows = _candidate_decisions(candidates, config, selected_date)
    decision_rows.extend(_position_decisions(paper_trades, candidates, config, selected_date))
    decisions = pd.DataFrame(decision_rows, columns=DECISION_COLUMNS)
    output_path = _write_decisions(report_dir, selected_date, decisions)
    return TradingDecisionResult(selected_date, decisions, output_path)


def decision_counts(decisions: pd.DataFrame) -> dict[str, int]:
    result = {
        "buy_candidate_count": 0,
        "watch_only_count": 0,
        "no_trade_count": 0,
        "hold_count": 0,
        "reduce_count": 0,
        "exit_review_count": 0,
        "grade_a_count": 0,
        "grade_b_count": 0,
        "grade_c_count": 0,
        "grade_d_count": 0,
    }
    if decisions.empty:
        return result
    decision = decisions["decision"].fillna("").astype(str)
    grade = decisions["candidate_grade"].fillna("").astype(str)
    result["buy_candidate_count"] = int((decision == "BUY_CANDIDATE").sum())
    result["watch_only_count"] = int((decision == "WATCH_ONLY").sum())
    result["no_trade_count"] = int((decision == "NO_TRADE").sum())
    result["hold_count"] = int((decision == "HOLD").sum())
    result["reduce_count"] = int((decision == "REDUCE").sum())
    result["exit_review_count"] = int((decision == "EXIT").sum())
    for letter in ["A", "B", "C", "D"]:
        result[f"grade_{letter.lower()}_count"] = int((grade == letter).sum())
    return result


def _candidate_decisions(candidates: pd.DataFrame, config: dict, trade_date: pd.Timestamp) -> list[dict]:
    decision_config = config.get("decision_engine", {}) if isinstance(config, dict) else {}
    min_confidence = float(decision_config.get("min_confidence_for_buy_candidate", 60))
    min_liquidity = float(decision_config.get("min_liquidity_for_buy_candidate", 50))
    min_grade = str(decision_config.get("min_grade_for_buy_candidate", "A")).strip().upper() or "A"
    block_high_event = bool(decision_config.get("block_high_event_risk", True))
    block_disposition = bool(decision_config.get("block_disposition_stock", True))
    block_stale_market_data = bool(decision_config.get("block_buy_candidate_when_market_data_stale", True))
    rows: list[dict] = []
    for _, row in candidates.iterrows():
        grade = str(row.get("candidate_grade", "") or grade_candidate(row).candidate_grade)
        risk_pass = _to_bool(row.get("risk_pass"))
        confidence = _num(row.get("confidence_score"), 50.0)
        liquidity = _num(row.get("liquidity_score"), 50.0)
        event_level = str(row.get("event_risk_level", "") or "").upper()
        disposition = _to_bool(row.get("is_disposition_stock"))
        blocked = (
            _to_bool(row.get("event_blocked"))
            or (block_high_event and event_level == "HIGH")
            or (block_disposition and disposition)
        )
        market_data_stale = block_stale_market_data and _market_data_is_stale(row)
        buy_candidate_ready = (
            _grade_value(grade) >= _grade_value(min_grade)
            and risk_pass
            and confidence >= min_confidence
            and liquidity >= min_liquidity
            and not blocked
        )
        if buy_candidate_ready and market_data_stale:
            decision, level, action = "WATCH_ONLY", "CAUTION", "observe_only"
            reason = "市場資料過期，暫不建立買進候選；原符合買進條件，降級為觀察名單"
        elif buy_candidate_ready:
            decision, level, action = "BUY_CANDIDATE", "WATCH", "review_before_entry"
            reason = "買進候選，需人工確認；不會自動下單"
        elif grade == "B":
            decision, level, action = "WATCH_ONLY", "WATCH", "observe_only"
            reason = "B 級改列觀察名單，降低過度進場；需人工確認"
        elif grade == "C":
            decision, level, action = "WATCH_ONLY", "CAUTION", "observe_only"
            reason = "觀察名單，資料或風險條件需人工確認"
        else:
            decision, level, action = "NO_TRADE", "HIGH_RISK", "no_action"
            reason = "不交易名單，風險或資料條件不符合"
        rows.append(_base_row(row, trade_date, "candidate", "CANDIDATE", decision, level, action, grade, reason))
    return rows


def _position_decisions(
    paper_trades: pd.DataFrame,
    candidates: pd.DataFrame,
    config: dict,
    trade_date: pd.Timestamp,
) -> list[dict]:
    if paper_trades.empty or "status" not in paper_trades.columns:
        return []
    candidate_lookup = _candidate_lookup(candidates)
    rows: list[dict] = []
    open_positions = paper_trades[paper_trades["status"].fillna("").astype(str).str.upper() == "OPEN"].copy()
    for _, position in open_positions.iterrows():
        stock_id = str(position.get("stock_id", "")).strip()
        merged = _merge_position_context(position, candidate_lookup.get(stock_id))
        risk_light = str(merged.get("risk_light", "") or "").strip()
        near_stop = _near_stop_loss(merged, config)
        high_event = str(merged.get("event_risk_level", "") or "").upper() == "HIGH" or _to_bool(
            merged.get("is_disposition_stock")
        )
        partial_exit = str(merged.get("exit_reason", "") or "").lower() in {"take_profit_1", "take_profit_2"}
        if near_stop or high_event or _to_bool(merged.get("stop_loss_hit")):
            decision, level, action = "EXIT", "HIGH_RISK", "exit_signal_review"
            reason = "出場訊號檢查，需人工確認；不會自動賣出"
        elif (
            risk_light == "紅燈" or risk_light == "黃燈" or partial_exit or _num(merged.get("liquidity_score"), 50) < 50
        ):
            decision, level, action = "REDUCE", "CAUTION", "reduce_risk"
            reason = "持倉風險升高，需人工檢查部位風險"
        else:
            decision, level, action = "HOLD", "INFO", "review_holding"
            reason = "持倉未觸發出場檢查，維持觀察"
        grade = str(merged.get("candidate_grade", "") or "-")
        rows.append(_base_row(merged, trade_date, "position", "OPEN", decision, level, action, grade, reason))
    return rows


def _base_row(
    row: pd.Series | dict,
    trade_date: pd.Timestamp,
    source: str,
    current_status: str,
    decision: str,
    decision_level: str,
    action: str,
    grade: str,
    reason: str,
) -> dict:
    getter = row.get
    return {
        "decision_date": pd.Timestamp.today().strftime("%Y-%m-%d"),
        "trade_date": trade_date.strftime("%Y-%m-%d"),
        "stock_id": str(getter("stock_id", "")).strip(),
        "stock_name": str(getter("stock_name", "") or ""),
        "source": source,
        "current_status": current_status,
        "decision": decision,
        "decision_level": decision_level,
        "action": action,
        "candidate_grade": grade,
        "reason": reason + _reason_suffix(getter("grade_reason", "")),
        "risk_flags": _text(getter("grade_risk_flags", ""))
        or _text(getter("investment_risk_flags", ""))
        or _text(getter("blocking_risks", ""))
        or _text(getter("warning_signals", ""))
        or _text(getter("risk_flags", "")),
        "positive_signals": _text(getter("positive_signals", "")),
        "warning_signals": _text(getter("warning_signals", "")),
        "blocking_risks": _text(getter("blocking_risks", "")),
        "momentum_signal": _text(getter("momentum_signal", "")),
        "confidence_score": _num(getter("confidence_score"), 50.0),
        "total_score": _num(getter("total_score"), 0.0),
        "multi_factor_score": _num(getter("multi_factor_score"), 50.0),
        "final_market_score": _num(getter("final_market_score"), 50.0),
        "liquidity_score": _num(getter("liquidity_score"), 50.0),
        "sector_strength_score": _num(getter("sector_strength_score"), 50.0),
        "event_risk_level": _text(getter("event_risk_level", "NONE")) or "NONE",
        "position_size_suggestion": _num(getter("suggested_position_pct"), 0.0),
        "can_auto_trade": False,
        "requires_manual_review": True,
        "review_level": _text(getter("review_level", "")) or "STANDARD_REVIEW",
        "review_reason": _text(getter("review_reason", "")) or "一般人工確認",
        "data_quality_flags": _text(getter("data_quality_flags", "")),
        "investment_risk_flags": _text(getter("investment_risk_flags", "")),
        "data_quality_note": _data_quality_note(row),
    }


def _candidate_lookup(candidates: pd.DataFrame) -> dict[str, pd.Series]:
    if candidates.empty or "stock_id" not in candidates.columns:
        return {}
    output = {}
    for _, row in candidates.iterrows():
        output[str(row.get("stock_id", "")).strip()] = row
    return output


def _merge_position_context(position: pd.Series, candidate: pd.Series | None) -> pd.Series:
    merged = position.copy()
    if candidate is None:
        return merged
    for column, value in candidate.items():
        if column not in merged.index or _is_blank(merged.get(column)):
            merged[column] = value
    return merged


def _near_stop_loss(row: pd.Series, config: dict) -> bool:
    local = config.get("local_factors", {}) if isinstance(config, dict) else {}
    light = local.get("holding_risk_light", {}) if isinstance(local, dict) else {}
    threshold = float(light.get("near_stop_loss_pct", 0.03))
    current = _num(row.get("current_price"), 0.0)
    stop = _num(row.get("stop_loss_price"), 0.0)
    if current <= 0 or stop <= 0:
        return False
    return ((current - stop) / current) <= threshold


def _data_quality_note(row: pd.Series | dict) -> str:
    text = "；".join(
        _text(row.get(column, ""))
        for column in [
            "data_quality_flags",
            "market_intel_warning",
            "data_source_warning",
            "financial_warning",
            "valuation_warning",
        ]
        if _text(row.get(column, ""))
    )
    return text or "資料品質已檢查；仍需人工確認"


def _market_data_is_stale(row: pd.Series | dict) -> bool:
    freshness_level = _text(row.get("data_freshness_level", "")).upper()
    if freshness_level in {"STALE", "CACHE"}:
        return True
    market_status = _text(row.get("market_intel_status", "")).upper()
    if market_status == "CACHE":
        return True
    return _to_bool(row.get("is_stale_data"))


def _reason_suffix(value: object) -> str:
    text = _text(value)
    return f"；{text}" if text else ""


def _write_decisions(report_dir: Path, trade_date: pd.Timestamp, decisions: pd.DataFrame) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"trading_decisions_{trade_date.strftime('%Y%m%d')}.csv"
    decisions.to_csv(path, index=False, encoding="utf-8")
    return path


def _latest_file(report_dir: Path, pattern: str, trade_date: str | None = None) -> Path | None:
    if trade_date:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            target = report_dir / pattern.replace("*", parsed.strftime("%Y%m%d"))
            if target.exists():
                return target
    files = sorted(report_dir.glob(pattern), key=lambda path: _date_from_path(path) or pd.Timestamp.min)
    return files[-1] if files else None


def _date_from_path(path: Path) -> pd.Timestamp | None:
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}


def _num(value: object, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(parsed):
        return default
    return parsed


def _text(value: object) -> str:
    if _is_blank(value):
        return ""
    return str(value).strip()


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    try:
        return bool(pd.isna(value))
    except TypeError:
        return False


def _grade_value(value: str) -> int:
    return {"D": 0, "C": 1, "B": 2, "A": 3}.get(str(value).strip().upper(), -1)
