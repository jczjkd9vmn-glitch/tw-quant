"""Open position review summary reporting."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

import pandas as pd


POSITION_REVIEW_COLUMNS = [
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
]


@dataclass(frozen=True)
class PositionReviewResult:
    trade_date: pd.Timestamp | None
    review: pd.DataFrame
    output_path: Path | None
    warning: str = ""


def generate_position_review_summary(
    reports_dir: str | Path = "reports",
    config: dict | None = None,
    trade_date: str | pd.Timestamp | None = None,
) -> PositionReviewResult:
    report_dir = Path(reports_dir)
    trades_path = report_dir / "paper_trades.csv"
    trades = _read_csv(trades_path)
    selected_date = _resolve_trade_date(report_dir, trade_date)
    if trades.empty or "status" not in trades.columns:
        review = pd.DataFrame(columns=POSITION_REVIEW_COLUMNS)
        return _write_result(report_dir, selected_date, review, "paper_trades.csv not found or empty")

    open_positions = trades[trades["status"].fillna("").astype(str).str.upper() == "OPEN"].copy()
    decisions_path = _latest_file(report_dir, "trading_decisions_*.csv", selected_date or trade_date)
    decisions = _read_csv(decisions_path)
    review = build_position_review_summary(open_positions, decisions, config or {}, selected_date)
    warning = "no open positions" if review.empty else ""
    return _write_result(report_dir, selected_date, review, warning)


def build_position_review_summary(
    open_positions: pd.DataFrame,
    decisions: pd.DataFrame | None = None,
    config: dict | None = None,
    trade_date: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    if open_positions.empty:
        return pd.DataFrame(columns=POSITION_REVIEW_COLUMNS)

    decision_lookup = _position_decision_lookup(decisions)
    rows: list[dict[str, object]] = []
    for _, position in open_positions.iterrows():
        stock_id = str(position.get("stock_id", "")).strip()
        decision_row = decision_lookup.get(stock_id, {})
        decision = str(decision_row.get("decision", "") or "").strip().upper()
        near_stop = _near_stop_loss(position, config or {})
        near_take = _near_take_profit(position, config or {})
        stop_hit = _truthy(position.get("stop_loss_hit"))
        high_event = (
            str(decision_row.get("event_risk_level", position.get("event_risk_level", "")) or "").upper() == "HIGH"
        )
        risk_light = str(position.get("risk_light", "") or decision_row.get("risk_light", "") or "")

        exit_review = decision == "EXIT" or stop_hit or high_event or near_stop or risk_light == "紅燈"
        reduce = (decision == "REDUCE" or near_take or risk_light == "黃燈") and not exit_review
        hold = not exit_review and not reduce
        data_warning = _data_quality_warning(position) or _data_quality_warning(decision_row)
        review_reason = str(decision_row.get("review_reason", "") or position.get("holding_risk_reason", "") or "")

        rows.append(
            {
                "trade_date": _date_text(trade_date or decision_row.get("trade_date") or position.get("trade_date")),
                "stock_id": stock_id,
                "stock_name": str(position.get("stock_name", "") or decision_row.get("stock_name", "") or ""),
                "decision": decision or ("EXIT" if exit_review else "REDUCE" if reduce else "HOLD"),
                "hold": bool(hold),
                "reduce": bool(reduce),
                "exit_review": bool(exit_review),
                "near_stop_loss": bool(near_stop),
                "near_take_profit": bool(near_take),
                "data_quality_warning": bool(data_warning),
                "current_price": _to_float(position.get("current_price")),
                "stop_loss_price": _to_float(position.get("stop_loss_price")),
                "unrealized_pnl_pct": _to_float(position.get("unrealized_pnl_pct")),
                "risk_light": risk_light,
                "review_reason": review_reason,
            }
        )
    return pd.DataFrame(rows, columns=POSITION_REVIEW_COLUMNS)


def _write_result(
    report_dir: Path,
    trade_date: pd.Timestamp | None,
    review: pd.DataFrame,
    warning: str,
) -> PositionReviewResult:
    if trade_date is None:
        return PositionReviewResult(None, review, None, warning or "cannot resolve position review date")
    report_dir.mkdir(parents=True, exist_ok=True)
    output_path = report_dir / f"position_review_summary_{trade_date.strftime('%Y%m%d')}.csv"
    review.to_csv(output_path, index=False, encoding="utf-8")
    return PositionReviewResult(trade_date, review, output_path, warning)


def _position_decision_lookup(decisions: pd.DataFrame | None) -> dict[str, dict[str, object]]:
    if decisions is None or decisions.empty or "stock_id" not in decisions.columns:
        return {}
    frame = decisions.copy()
    frame["stock_id"] = frame["stock_id"].astype(str).str.strip()
    if "source" in frame.columns:
        position_rows = frame[frame["source"].fillna("").astype(str) == "position"].copy()
        if not position_rows.empty:
            frame = position_rows
    return {
        stock_id: row.to_dict() for stock_id, row in frame.drop_duplicates("stock_id").set_index("stock_id").iterrows()
    }


def _near_stop_loss(row: pd.Series, config: dict) -> bool:
    local = config.get("local_factors", {}) if isinstance(config, dict) else {}
    light = local.get("holding_risk_light", {}) if isinstance(local, dict) else {}
    threshold = _to_float(light.get("near_stop_loss_pct")) or 0.03
    current = _to_float(row.get("current_price"))
    stop = _to_float(row.get("stop_loss_price"))
    if current is None or stop is None or current <= 0:
        return False
    return ((current - stop) / current) <= threshold


def _near_take_profit(row: pd.Series, config: dict) -> bool:
    exit_config = config.get("exit_strategy", {}) if isinstance(config, dict) else {}
    thresholds = [
        _to_float(exit_config.get("take_profit_1_pct")) or 0.08,
        _to_float(exit_config.get("take_profit_2_pct")) or 0.15,
    ]
    proximity = _to_float(exit_config.get("take_profit_near_pct")) or 0.01
    pnl_pct = _to_float(row.get("unrealized_pnl_pct"))
    if pnl_pct is None:
        current = _to_float(row.get("current_price"))
        entry = _to_float(row.get("entry_price"))
        if current is None or entry is None or entry <= 0:
            return False
        pnl_pct = current / entry - 1
    return any(threshold - proximity <= pnl_pct < threshold for threshold in thresholds)


def _data_quality_warning(row: pd.Series | dict[str, object]) -> bool:
    for column in [
        "data_quality_flags",
        "data_quality_note",
        "market_intel_warning",
        "data_source_warning",
        "financial_warning",
        "valuation_warning",
        "liquidity_warning",
        "sector_strength_warning",
    ]:
        text = str(row.get(column, "") or "").strip()
        if (
            text
            and text.lower() != "nan"
            and any(keyword in text for keyword in ["資料不足", "警告", "WARNING", "CACHE"])
        ):
            return True
    return False


def _resolve_trade_date(report_dir: Path, trade_date: str | pd.Timestamp | None) -> pd.Timestamp | None:
    if trade_date is not None:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            return parsed
    summary_path = _latest_file(report_dir, "daily_summary_*.csv")
    summary = _read_csv(summary_path)
    if not summary.empty and "trade_date" in summary.columns:
        parsed = pd.to_datetime(summary["trade_date"].iloc[0], errors="coerce")
        if not pd.isna(parsed):
            return parsed
    decisions_path = _latest_file(report_dir, "trading_decisions_*.csv")
    return _date_from_path(decisions_path) if decisions_path is not None else None


def _latest_file(report_dir: Path, pattern: str, trade_date: str | pd.Timestamp | None = None) -> Path | None:
    if trade_date is not None:
        parsed = pd.to_datetime(trade_date, errors="coerce")
        if not pd.isna(parsed):
            target = report_dir / pattern.replace("*", parsed.strftime("%Y%m%d"))
            if target.exists():
                return target
    files = sorted(report_dir.glob(pattern), key=lambda path: _date_from_path(path) or pd.Timestamp.min)
    return files[-1] if files else None


def _date_from_path(path: Path | None) -> pd.Timestamp | None:
    if path is None:
        return None
    match = re.search(r"_(\d{8})\.csv$", path.name)
    if not match:
        return None
    parsed = pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce")
    return None if pd.isna(parsed) else parsed


def _date_text(value: object) -> str:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是"}


def _to_float(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(number):
        return None
    return number


def _read_csv(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8", dtype={"stock_id": str})
    except Exception:
        return pd.DataFrame()
